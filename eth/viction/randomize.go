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
	"math/rand"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state/contracts"
	"github.com/erigontech/erigon/execution/abi/bind"
)

func GetAttestors(vicConfig *chain.VictionConfig, validators []common.Address, client bind.ContractBackend) ([]int64, error) {
	randomizes := []int64{}
	validatorCount := int64(len(validators))
	if validatorCount > 0 {
		for _, validator := range validators {
			random, err := GetRandomizeOfValidator(vicConfig, validator, client)
			if err != nil {
				return nil, err
			}
			randomizes = append(randomizes, random)
		}
		attestors, err := GetAttestorsFromRandomize(randomizes, validatorCount)
		if err != nil {
			return nil, err
		}
		return attestors, nil
	}
	return nil, ErrNoValidator
}

func GetAttestorsFromRandomize(randomizes []int64, signersLen int64) ([]int64, error) {
	randomSeed := int64(0)
	for _, j := range randomizes {
		randomSeed += j
	}
	rand.Seed(randomSeed)

	randArray := GenerateSequence(0, 1, signersLen)
	attestorIndices := make([]int64, signersLen)
	attestorIndex := int64(0)
	for i := len(randArray) - 1; i >= 0; i-- {
		blockLength := len(randArray) - 1
		if blockLength <= 1 {
			blockLength = 1
		}
		randomIndex := int64(rand.Intn(blockLength))
		attestorIndex = randArray[randomIndex]
		randArray[randomIndex] = randArray[i]
		randArray[i] = attestorIndex
		randArray = append(randArray[:i], randArray[i+1:]...)
		attestorIndices[i] = attestorIndex
	}

	return attestorIndices, nil
}

func GetRandomizeOfValidator(vicConfig *chain.VictionConfig, validator common.Address, client bind.ContractBackend) (int64, error) {
	randomize, err := contracts.NewVictionRandomize(vicConfig.RandomizerContract, client)
	if err != nil {
		return -1, err
	}

	opts := new(bind.CallOpts)
	secrets, err := randomize.GetSecret(opts, validator)
	if err != nil {
		return -1, err
	}
	opening, err := randomize.GetOpening(opts, validator)
	if err != nil {
		return -1, err
	}

	return DecryptRandomize(secrets, opening)
}
