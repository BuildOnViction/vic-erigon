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

package eth

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/viction"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/posv"
	"github.com/erigontech/erigon/rpc/rpchelper"

	"github.com/tforce-io/tf-golib/stdx/mathxt/bigxt"
)

// Get attestors from list of validators at checkpoint block.
func (s *Ethereum) PosvGetAttestors(vicConfig *chain.VictionConfig, header *types.Header, validators []common.Address,
) ([]int64, error) {
	return viction.GetAttestors(vicConfig, validators, s.contractBackend)
}

// Get block signers from the state.
func (s *Ethereum) PosvGetBlockSignData(config *chain.Config, vicConfig *chain.VictionConfig, header *types.Header,
	chain consensus.ChainReader,
) []types.Transaction {
	blockNumber := header.Number.Uint64()
	block := chain.GetBlock(header.Hash(), blockNumber)
	data := []types.Transaction{}
	transactions := block.Transactions()
	if config.IsTIPSigning(blockNumber) {
		for _, tx := range transactions {
			if IsVicBlockSingingTx(tx, vicConfig) {
				data = append(data, tx)
			}
		}
	} else {
		// TODO: Handle receipts later
		for _, tx := range transactions {
			if IsVicBlockSingingTx(tx, vicConfig) {
				data = append(data, tx)
			}
		}
	}
	return data
}

// Get creator-attestor pairs from the state.
func (s *Ethereum) PosvGetCreatorAttestorPairs(c *posv.Posv, config *chain.Config,
	header, checkpointHeader *types.Header,
) (map[common.Address]common.Address, uint64, error) {
	return viction.GetCreatorAttestorPairs(c, config, config.Posv, header, checkpointHeader)
}

// Calculate and distribute reward at checkpoint block.
func (s *Ethereum) PosvGetEpochReward(c *posv.Posv, config *chain.Config, posvConfig *chain.PosvConfig, vicConfig *chain.VictionConfig,
	header *types.Header,
	chain consensus.ChainReader, state *state.IntraBlockState, logger log.Logger,
) (*posv.EpochReward, error) {
	epochRewards := &posv.EpochReward{}
	blockNumber := header.Number.Uint64()
	blockNumberBig := header.Number

	if bigxt.IsLessThanOrEqualInt(blockNumberBig, new(big.Int).SetUint64(posvConfig.Epoch)) {
		return epochRewards, nil
	}

	// Get initial reward
	totalReward := viction.CalcDefaultRewardPerBlock((*big.Int)(vicConfig.RewardPerEpoch), blockNumber, posvConfig.BlocksPerYear())
	// Get additional reward for Saigon upgrade
	if chain.Config().IsSaigon(blockNumber) {
		saigonReward := viction.CalcSaigonRewardPerBlock((*big.Int)(vicConfig.SaigonRewardPerEpoch), chain.Config().SaigonBlock, blockNumber, posvConfig.BlocksPerYear())
		totalReward = new(big.Int).Add(totalReward, saigonReward)
	}

	// Calculate rewards for validators and stakeholders
	validatorRewards, _ := viction.CalcRewardsForValidators(c, config, posvConfig, vicConfig, header, totalReward, chain, logger)
	epochRewards.ValidatorRewards = validatorRewards

	stakeholderRewards, _ := viction.CalcRewardsForStakeholders(c, config, posvConfig, vicConfig, header, validatorRewards, state, logger)
	epochRewards.StakholderRewards = stakeholderRewards

	return epochRewards, nil
}

// Get list of validators creating bad block or not creating block at all.
func (s *Ethereum) PosvGetPenalties(c *posv.Posv, config *chain.Config, posvConfig *chain.PosvConfig, vicConfig *chain.VictionConfig,
	header *types.Header,
	chain consensus.ChainReader,
) ([]common.Address, error) {
	if config.IsTIPSigning(header.Number.Uint64()) {
		return viction.PenalizeValidatorsTIPSigning(c, config, posvConfig, vicConfig, header, chain)
	}
	return viction.PenalizeValidatorsDefault(c, config, posvConfig, vicConfig, header, chain)
}

// Get eligble validators from the state.
func (s *Ethereum) PosvGetValidators(vicConfig *chain.VictionConfig, header *types.Header, chain consensus.ChainReader,
) ([]common.Address, error) {
	fmt.Println("-> PosvGetValidators", "header", header.Hash().Hex(), "number", header.Number.Uint64())
	tx, _ := s.chainDB.BeginTemporalRo(context.TODO())
	defer tx.Rollback()

	// During header verification, the block body may not exist yet
	// Use the header's number directly to create the state reader
	blockNumber := header.Number.Uint64()
	reader, err := rpchelper.CreateHistoryStateReader(tx, blockNumber, 0, rawdbv3.TxNums)
	if err != nil {
		return nil, fmt.Errorf("failed to create history state reader: %w", err)
	}
	state := state.New(reader)
	return viction.GetValidators(vicConfig, state, s.contractBackend)
}

// Return a state.IntraBlockState instance to access low level contract storage.
func (s *Ethereum) GetStateReader(tx kv.TemporalTx) *state.IntraBlockState {
	reader := state.NewReaderV3(tx)
	return state.New(reader)
}

// Return a state.IntraBlockState instance to access low level contract storage.
func (s *Ethereum) GetHistoricalStateReader(tx kv.TemporalTx, block *types.Block) *state.IntraBlockState {
	if block == nil {
		// During header verification, block may be nil
		// This function should not be called in that context
		return nil
	}
	fmt.Println("-> GetHistoricalStateReader", "block", block.NumberU64(), "hash", block.Hash().Hex())
	reader, _ := rpchelper.CreateHistoryStateReader(tx, block.NumberU64(), 0, rawdbv3.TxNums)
	return state.New(reader)
}

// Check a transaction is Viction BlockSign transaction.
func IsVicBlockSingingTx(tx types.Transaction, vicConfig *chain.VictionConfig) bool {
	toAddr := tx.GetTo()
	if toAddr == nil || *toAddr != vicConfig.ValidatorBlockSignContract {
		return false
	}

	data := tx.GetData()
	method := common.Bytes2Hex(data[0:4])

	if method != state.SignMethodHex && len(data) >= 68 {
		return false
	}

	return true
}
