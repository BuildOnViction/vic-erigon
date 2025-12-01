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
	"math/big"

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

type EpochReward struct {
	ValidatorRewards  map[common.Address]*ValidatorReward `json:"signers"`
	StakholderRewards map[common.Address]*big.Int         `json:"rewards"`
}

type ValidatorReward struct {
	Sign   uint64   `json:"sign"`
	Reward *big.Int `json:"reward"`
}

type PosvBackend interface {
	// Calculate and distribute reward at the end of each epoch.
	PosvEpochReward(c *Posv, config *chain.Config, posvConfig *chain.PosvConfig, vicConfig *chain.VictionConfig,
		header *types.Header, state *state.IntraBlockState,
		txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
		chain consensus.ChainReader, syscall consensus.SystemCall, skipReceiptsEval bool, logger log.Logger,
	) (*EpochReward, error)

	// Penalize validators for creating bad block or not creating block at all.
	PosvPenalize()

	// Get eligble validators from the state.
	PosvGetValidators()

	// Get attestors from list of validators.
	PosvGetAttestors()

	// Get block signers from the state.
	PosvGetBlockSignData(config *chain.Config, vicConfig *chain.VictionConfig, header *types.Header,
		chain consensus.ChainReader,
	) []types.Transaction

	// Verify list of new validators for next epoch.
	PosvVerifyNewValidators()
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

// Process block header Extra field of a checkpoint block to return the list of new validators.
func ExtractValidatorsFromCheckpointHeader(header *types.Header) []common.Address {
	if header == nil {
		return []common.Address{}
	}

	validators := make([]common.Address, (len(header.Extra)-ExtraVanity-ExtraSeal)/int(addressLength))
	for i := 0; i < len(validators); i++ {
		copy(validators[i][:], header.Extra[ExtraVanity+i*int(addressLength):])
	}

	return validators
}
