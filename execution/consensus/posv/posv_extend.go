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
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/consensus"
)

const (
	attestorHeaderItemLength = 4
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

// Recover the signer address from a block header
func (c *Posv) Ecrecover(header *types.Header) (common.Address, error) {
	return ecrecover(header, c.signatures)
}

// Process block header Extra field of a checkpoint block to return the list of new validators.
func ExtractValidatorsFromCheckpointHeader(header *types.Header) []common.Address {
	if header == nil {
		return []common.Address{}
	}

	validators := make([]common.Address, (len(header.Extra)-ExtraVanity-ExtraSeal)/int(AddressLength))
	for i := 0; i < len(validators); i++ {
		copy(validators[i][:], header.Extra[ExtraVanity+i*int(AddressLength):])
	}

	return validators
}

// Encode list of attestor numbers into bytes following format of Block.Attestors.
func EncodeAttestorsForHeader(attestors []uint64) []byte {
	var attestorsBuff []byte
	for _, attestor := range attestors {
		m2Byte := common.LeftPadBytes([]byte(fmt.Sprintf("%d", attestor)), attestorHeaderItemLength)
		attestorsBuff = append(attestorsBuff, m2Byte...)
	}
	return attestorsBuff
}

// Decode bytes with format of Block.Attestors into list of attestor numbers.
func DecodeAttestorsFromHeader(attestorsBuff []byte) []uint64 {
	var attestors []uint64
	attestorCount := len(attestorsBuff) / attestorHeaderItemLength
	for i := 0; i < attestorCount; i++ {
		attestorBuff := bytes.Trim(attestorsBuff[i*attestorHeaderItemLength:(i+1)*attestorHeaderItemLength], "\x00")
		attestorNumber, err := strconv.ParseUint(string(attestorBuff), 10, 64)
		if err != nil {
			return []uint64{}
		}
		attestors = append(attestors, attestorNumber)
	}

	return attestors
}
