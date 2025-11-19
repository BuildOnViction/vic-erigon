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

// Calculate and distribute reward at the end of each epoch.
func (s *Ethereum) PosvEpochReward()

// Penalize validators for creating bad block or not creating block at all.
func (s *Ethereum) PosvPenalize()

// Get eligble validators from the state.
func (s *Ethereum) PosvGetValidators()

// Get attestors from list of validators.
func (s *Ethereum) PosvGetAttestors()

// Verify list of new validators for next epoch.
func (s *Ethereum) PosvVerifyNewValidators()
