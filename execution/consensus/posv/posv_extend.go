// Copyright 2025 The Viction Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package posv

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/consensus"
	lru "github.com/hashicorp/golang-lru/arc/v2"
)

const (
	attestorHeaderItemLength = 4
)

var (
	errInvalidBlockAttestor        = fmt.Errorf("invalid block attestor")
	errEmptyValidators             = fmt.Errorf("validators is empty")
	errInvalidCheckpointPenalties  = fmt.Errorf("invalid penalty list on checkpoint block")
	errInvalidCheckpointValidators = fmt.Errorf("invalid validator list on checkpoint block")
)

// EpochReward stores number of sign made by each validator and rewards for
// all stakeholders (validators and voters) in an epoch.
type EpochReward struct {
	ValidatorRewards  map[common.Address]*ValidatorReward `json:"signers"`
	StakholderRewards map[common.Address]*big.Int         `json:"rewards"`
}

// ValidatorInfo stores basic information about a validator.
type ValidatorInfo struct {
	Address  common.Address `json:"address"`
	Capacity *big.Int       `json:"capacity"`
	Owner    common.Address `json:"owner"`
}

type ValidatorReward struct {
	Sign   uint64   `json:"sign"`
	Reward *big.Int `json:"reward"`
}

type PosvBackend interface {
	// Get attestors from list of validators.
	PosvGetAttestors(vicConfig *chain.VictionConfig, header *types.Header, validators []common.Address,
	) ([]int64, error)

	// Get block signers from the state.
	PosvGetBlockSignData(config *chain.Config, vicConfig *chain.VictionConfig, header *types.Header,
		chain consensus.ChainReader,
	) []types.Transaction

	// Get creator-attestor pairs from the state.
	PosvGetCreatorAttestorPairs(c *Posv, config *chain.Config,
		header, checkpointHeader *types.Header,
	) (map[common.Address]common.Address, uint64, error)

	// Calculate and distribute reward at the end of each epoch.
	PosvGetEpochReward(c *Posv, config *chain.Config, posvConfig *chain.PosvConfig, vicConfig *chain.VictionConfig,
		header *types.Header,
		chain consensus.ChainReader, state *state.IntraBlockState, logger log.Logger,
	) (*EpochReward, error)

	// Penalize validators for creating bad block or not creating block at all.
	PosvGetPenalties(c *Posv, config *chain.Config, posvConfig *chain.PosvConfig, vicConfig *chain.VictionConfig,
		header *types.Header,
		chain consensus.ChainReader,
	) ([]common.Address, error)

	// Get eligble validators from the state.
	PosvGetValidators(vicConfig *chain.VictionConfig, header *types.Header, chain consensus.ChainReader,
	) ([]common.Address, error)
}

// Check If the given block is a checkpoint block, return it, else return previous checkpoint block header.
func GetCheckpointHeader(posvConfig *chain.PosvConfig, header *types.Header, chain consensus.ChainHeaderReader) *types.Header {
	blockNumber := header.Number.Uint64()
	if blockNumber%posvConfig.Epoch == 0 {
		return header
	}
	prevCheckpointBlockNumber := blockNumber - (blockNumber % posvConfig.Epoch)
	prevCheckpointHeader := chain.GetHeaderByNumber(prevCheckpointBlockNumber)
	return prevCheckpointHeader
}

// Get all BlockSign transactions for a given block. If it's not cached yet, get it from the state.
func (c *Posv) GetSignDataForBlock(config *chain.Config, vicConfig *chain.VictionConfig, header *types.Header,
	chain consensus.ChainReader) []types.Transaction {
	blockHash := header.Hash()
	if signers, ok := c.blockSigners.Get(blockHash); ok {
		return signers
	}
	signers := c.backend.PosvGetBlockSignData(config, vicConfig, header, chain)
	c.blockSigners.Add(blockHash, signers)
	return signers
}

// Check if the signer is inturn to mint current block. Also return context of the check including:
// currentIndex, parentIndex, validatorCount.
func (c *Posv) IsMyTurn(signer common.Address, parentNumber uint64, parentHash common.Hash, chain consensus.ChainHeaderReader) (bool, int, int, int, error) {
	parent := chain.GetHeader(parentHash, parentNumber)
	checkpointHeader := GetCheckpointHeader(c.ChainConfig.Posv, parent, chain)
	validators := ExtractValidatorsFromCheckpointHeader(checkpointHeader)
	validatorsCount := len(validators)
	if validatorsCount == 0 {
		return false, -1, -1, 0, errEmptyValidators
	}

	parentIndex := -1
	if parentNumber > 0 {
		parentCreator, err := c.Author(parent)
		if err != nil {
			return false, 0, 0, 0, err
		}
		parentIndex = common.IndexOf(validators, parentCreator)
	}
	currentIndex := common.IndexOf(validators, signer)

	inturn := (parentIndex+1)%validatorsCount == currentIndex
	return inturn, currentIndex, parentIndex, validatorsCount, nil
}

func (c *Posv) calcDifficulty(signer common.Address, parentNumber uint64, parentHash common.Hash, chain consensus.ChainHeaderReader) *big.Int {
	_, currentIndex, parentIndex, validatorCount, err := c.IsMyTurn(signer, parentNumber, parentHash, chain)
	if err == nil {
		distance := Distance(currentIndex, parentIndex, validatorCount)
		return big.NewInt(int64(validatorCount - distance + 1))
	}
	return big.NewInt(int64(validatorCount + currentIndex - parentIndex))
}

// Recover the attesor address from a block header
func (c *Posv) Attestor(header *types.Header) (common.Address, error) {
	return ecrecover2(header, c.attestSignatures)
}

// ecrecover2 extracts the Ethereum account address from a Attestor header.
func ecrecover2(header *types.Header, sigcache *lru.ARCCache[common.Hash, common.Address]) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()

	// hitrate while straight-forward sync is from 0.5 to 0.65
	if address, known := sigcache.Peek(hash); known {
		return address, nil
	}

	// Retrieve the signature from the header extra-data
	if len(header.Attestor) != ExtraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Attestor

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(SealHash(header).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}

	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)
	return signer, nil
}

// Decode bytes with format of Block.Attestors into list of attestor numbers.
func DecodeAttestorsFromHeader(attestorsBuff []byte) []int64 {
	attestorCount := len(attestorsBuff) / attestorHeaderItemLength
	attestors := make([]int64, attestorCount)
	for i := 0; i < attestorCount; i++ {
		attestorBuff := bytes.Trim(attestorsBuff[i*attestorHeaderItemLength:(i+1)*attestorHeaderItemLength], "\x00")
		attestorNumber, err := strconv.ParseInt(string(attestorBuff), 10, 64)
		if err != nil {
			return []int64{}
		}
		attestors[i] = attestorNumber
	}

	return attestors
}

// Decode bytes with format of Block.Penalties into list of addresses.
func DecodePenaltiesFromHeader(penaltiesBuff []byte) []common.Address {
	addressLengthInt := int(AddressLength)
	penaltyCount := len(penaltiesBuff) / addressLengthInt
	penalties := make([]common.Address, penaltyCount)
	for i := 0; i < penaltyCount; i++ {
		penaltyBuff := penaltiesBuff[i*addressLengthInt : (i+1)*addressLengthInt]
		penalties[i] = common.BytesToAddress(penaltyBuff)
	}
	return penalties
}

// Return the distance between current index and parent index in the circular list of validators.
func Distance(currentIndex, parentIndex, validatorCount int) int {
	if currentIndex > parentIndex {
		return currentIndex - parentIndex
	}
	return validatorCount + currentIndex - parentIndex
}

// Process block header NewAttestors field of a checkpoint block to return the list of new attestors.
func ExtractAttestorsFromCheckpointHeader(header *types.Header) []int64 {
	if header == nil {
		return []int64{}
	}

	attestors := DecodeAttestorsFromHeader(header.NewAttestors)
	return attestors
}

// Process block header Extra field of a checkpoint block to return the list of new validators.
func ExtractValidatorsFromCheckpointHeader(header *types.Header) []common.Address {
	if header == nil {
		return []common.Address{}
	}

	// Check if Extra field has enough bytes for at least ExtraVanity + ExtraSeal
	if len(header.Extra) < ExtraVanity+ExtraSeal {
		return []common.Address{}
	}

	// Calculate number of validators: (total - vanity - seal) / address_length
	validatorBytes := len(header.Extra) - ExtraVanity - ExtraSeal
	if validatorBytes < 0 || validatorBytes%int(AddressLength) != 0 {
		return []common.Address{}
	}

	validatorCount := validatorBytes / int(AddressLength)
	validators := make([]common.Address, validatorCount)
	for i := 0; i < validatorCount; i++ {
		copy(validators[i][:], header.Extra[ExtraVanity+i*int(AddressLength):])
	}

	return validators
}

// Encode list of attestor numbers into bytes following format of Block.Attestors.
func EncodeAttestorsForHeader(attestors []int64) []byte {
	var attestorsBuff []byte
	for _, attestor := range attestors {
		attestorBuff := common.LeftPadBytes([]byte(fmt.Sprintf("%d", attestor)), attestorHeaderItemLength)
		attestorsBuff = append(attestorsBuff, attestorBuff...)
	}
	return attestorsBuff
}

// Encode list of penalized addresses into bytes following format of Block.Penalties.
func EncodePenaltiesForHeader(penalties []common.Address) []byte {
	var penaltiesBuff []byte
	for _, attestor := range penalties {
		penaltiesBuff = append(penaltiesBuff, attestor.Bytes()...)
	}
	return penaltiesBuff
}
