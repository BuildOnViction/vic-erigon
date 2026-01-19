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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/execution/consensus"
)

// Enhance verifyHeader by caching the result to speed up repeated verifications.
func (c *Posv) verifyHeaderWithCache(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	_, ok := c.verifiedBlocks.Get(header.Hash())
	if ok {
		return nil
	}

	err := c.verifyHeader(chain, header, parents)
	if err == nil {
		c.verifiedBlocks.Add(header.Hash(), true)
	}
	return err
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements.
func (c *Posv) verifySeal(chainH consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {
	chain := chainH.(consensus.ChainReader)

	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {

		return errUnknownBlock
	}

	// Resolve the authorization key and check against signers
	validators, err := c.backend.PosvGetValidators(c.ChainConfig.Viction, header, chain)
	if err != nil {
		fmt.Println("-> verifySeal", "number", number, "err", err)
		return err
	}
	creator, err := ecrecover(header, c.signatures)
	if err != nil {
		fmt.Println("-> verifySeal", "number", number, "err", err)
		return err
	}

	if _, ok := snap.Signers[creator]; !ok {
		if common.IndexOf(validators, creator) == -1 {
			return ErrUnauthorizedSigner
		}
	}

	for seen, recent := range snap.Recents {
		if len(validators) <= 1 {
			break
		}
		if recent == creator {
			// Signer is among RecentsRLP, only fail if the current block doesn't shift it out
			// There is only case that we don't allow signer to create two continuous blocks.
			if limit := uint64(2); seen > number-limit {
				// Only take into account the non-epoch blocks
				if number%c.config.Epoch != 0 {
					return ErrUnauthorizedSigner
				}
			}
		}
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	parent := chain.GetHeader(header.ParentHash, number-1)
	difficulty := c.calcDifficulty(creator, parent.Number.Uint64(), parent.Hash(), chain)
	log.Info("-> verifySeal", "number", number, "difficulty", header.Difficulty, "difficulty", difficulty.Int64())
	if header.Difficulty.Int64() != difficulty.Int64() {
		c.logger.Info("-> verifySeal", "number", number, "difficulty", header.Difficulty, "difficulty", difficulty.Int64())
		return errInvalidDifficulty
	}

	// Enforce double validation
	if number > c.config.Epoch {
		attestor, err := c.Attestor(header)
		if err != nil {
			return err
		}

		checkpointHeader := GetCheckpointHeader(c.config, parent, chain)
		valAttPairs, _, err := c.backend.PosvGetCreatorAttestorPairs(c, c.ChainConfig, header, checkpointHeader)
		if err != nil {
			fmt.Println("-> verifySeal", "number", number, "err", err)
			return err
		}
		assignedAttestor, ok := valAttPairs[creator]
		if !ok || attestor != assignedAttestor {
			return errInvalidBlockAttestor
		}
	}

	return nil
}

// Verify the current validators list at checkpoint block are comformed to the consensus rules.
// Error with be returned violations are found.
func (c *Posv) verifyValidators(chain consensus.ChainReader, header *types.Header, parents []*types.Header) error {
	number := header.Number.Uint64()
	snap, err := c.Snapshot(chain, header.Number.Uint64()-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	validators := snap.GetSigners()
	retryCount := 0
	for retryCount < 2 {
		// compare penalties computed from state with header.Penalties
		penalties, err := c.backend.PosvGetPenalties(c, c.ChainConfig, c.ChainConfig.Posv, c.ChainConfig.Viction, header, chain)
		if err != nil {
			return err
		}
		penaltiesBuff := EncodePenaltiesForHeader(penalties)
		if !bytes.Equal(penaltiesBuff, header.Penalties) {
			return errInvalidCheckpointPenalties
		}

		// remove penalized validators in current epoch
		if len(penalties) > 0 {
			validators = common.SetSubstract(validators, penalties)
			header.Penalties = EncodePenaltiesForHeader(penalties)
		}
		// remove penalized validators in recent epochs
		for i := uint64(1); i <= c.ChainConfig.Viction.PenaltyEpochCount; i++ {
			prevCheckpointBlockNumber := number - (i * c.config.Epoch)
			prevCehckpointHeader := chain.GetHeaderByNumber(prevCheckpointBlockNumber)
			penalties := DecodePenaltiesFromHeader(prevCehckpointHeader.Penalties)
			if len(penalties) > 0 {
				validators = common.SetSubstract(validators, penalties)
			}
		}
		// compare validators computed from state with header.Extra
		headerValidators := ExtractValidatorsFromCheckpointHeader(header)
		if common.AreSimilarSlices(validators, headerValidators) {
			break
		}

		// if not matched, try to get validators from smart contract and verify again
		if retryCount == 0 {
			gapBlockNumber := number - c.config.Gap
			gapBlockHeader := chain.GetHeaderByNumber(gapBlockNumber)
			validators, err = c.backend.PosvGetValidators(c.ChainConfig.Viction, gapBlockHeader, chain)
			if err != nil {
				return err
			}
		}
		// maximum retry reached, return error
		if retryCount == 1 {
			return errInvalidCheckpointValidators
		}

		retryCount++
	}

	return nil
}
