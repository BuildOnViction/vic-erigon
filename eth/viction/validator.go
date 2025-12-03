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

package viction

import (
	"sort"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/state/contracts"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/consensus/posv"
	"github.com/tforce-io/tf-golib/stdx/mathxt/bigxt"
)

// Get eligble validators from the state.
//
// *NOTE: The injected state must be at the checkpoint block.
func GetValidators(vicConfig *chain.VictionConfig, state *state.IntraBlockState, client bind.ContractBackend,
) ([]common.Address, error) {
	addresses := state.VicGetCandidates(vicConfig.ValidatorContract)
	validatorContract, _ := contracts.NewVictionValidator(vicConfig.ValidatorContract, client)

	opts := new(bind.CallOpts)
	candidates := []*posv.ValidatorInfo{}
	for _, addr := range addresses {
		cap, err := validatorContract.GetCandidateCap(opts, addr)
		if err != nil {
			return nil, err
		}
		if addr == (common.Address{}) {
			continue
		}
		candidates = append(candidates, &posv.ValidatorInfo{Address: addr, Capacity: cap})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return bigxt.IsGreaterThanOrEqualInt(candidates[i].Capacity, candidates[j].Capacity)
	})
	validatorMaxCountInt := int(vicConfig.ValidatorMaxCount)
	if len(candidates) > validatorMaxCountInt {
		candidates = candidates[:validatorMaxCountInt]
	}
	validators := []common.Address{}
	for _, candidate := range candidates {
		validators = append(validators, candidate.Address)
	}
	return validators, nil
}
