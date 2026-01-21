// Copyright 2024 The Erigon Authors
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

package rpchelper

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	borfinality "github.com/erigontech/erigon/polygon/bor/finality"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/holiman/uint256"
)

// unable to decode supplied params, or an invalid number of parameters
type nonCanonocalHashError struct{ hash common.Hash }

func (e nonCanonocalHashError) ErrorCode() int { return -32603 }

func (e nonCanonocalHashError) Error() string {
	return fmt.Sprintf("hash %x is not currently canonical", e.hash)
}

type BlockNotFoundErr struct {
	Hash common.Hash
}

func (e BlockNotFoundErr) Error() string {
	return fmt.Sprintf("block %x not found", e.Hash)
}

func GetBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, _, err := _GetBlockNumber(ctx, blockNrOrHash.RequireCanonical, blockNrOrHash, tx, br, filters)
	return bn, bh, latest, err
}

func GetCanonicalBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, _, err := _GetBlockNumber(ctx, blockNrOrHash.RequireCanonical, blockNrOrHash, tx, br, filters)
	return bn, bh, latest, err
}

func _GetBlockNumber(ctx context.Context, requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (blockNumber uint64, hash common.Hash, latest bool, found bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
	// from the block number corresponding to the plain state
	var plainStateBlockNumber uint64
	if plainStateBlockNumber, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return 0, common.Hash{}, false, false, fmt.Errorf("getting plain state block number: %w", err)
	}
	var ok bool
	hash, ok = blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		switch number {
		case rpc.LatestBlockNumber:
			if blockNumber, err = GetLatestBlockNumber(tx); err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.EarliestBlockNumber:
			blockNumber = 0
		case rpc.FinalizedBlockNumber:
			if whitelist.GetWhitelistingService() != nil {
				num := borfinality.GetFinalizedBlockNumber(tx)
				if num == 0 {
					// nolint
					return 0, common.Hash{}, false, false, errors.New("No finalized block")
				}

				blockNum := borfinality.CurrentFinalizedBlock(tx, num).NumberU64()
				blockHash := rawdb.ReadHeaderByNumber(tx, blockNum).Hash()
				return blockNum, blockHash, false, false, nil
			}
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.SafeBlockNumber:
			blockNumber, err = GetSafeBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.PendingBlockNumber:
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber = plainStateBlockNumber
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), false, true, nil
			}
		case rpc.LatestExecutedBlockNumber:
			blockNumber = plainStateBlockNumber
		default:
			blockNumber = uint64(number.Int64())
		}
		hash, ok, err = br.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if !ok { //future blocks must behave as "latest"
			return blockNumber, hash, blockNumber == plainStateBlockNumber, true, nil
		}
	} else {
		number, err := br.HeaderNumber(ctx, tx, hash)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if number == nil {
			return 0, common.Hash{}, false, false, BlockNotFoundErr{Hash: hash}
		}
		blockNumber = *number

		ch, ok, err := br.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if requireCanonical && (!ok || ch != hash) {
			return 0, common.Hash{}, false, false, nonCanonocalHashError{hash}
		}
	}
	return blockNumber, hash, blockNumber == plainStateBlockNumber, true, nil
}

func CreateStateReader(ctx context.Context, tx kv.TemporalTx, br services.FullBlockReader, blockNrOrHash rpc.BlockNumberOrHash, txnIndex int, filters *Filters, stateCache kvcache.Cache, txNumReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	blockNumber, _, latest, _, err := _GetBlockNumber(ctx, true, blockNrOrHash, tx, br, filters)
	if err != nil {
		return nil, err
	}

	reader, err := CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, txnIndex, stateCache, txNumReader)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// func CreateStateReaderFromBlockNumber(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, latest bool, txnIndex int, stateCache kvcache.Cache, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
// 	// For block 0 (genesis), try using ReaderV3 which reads from latest state
// 	// Genesis state should be in the latest state since it's the base state
// 	if blockNumber == 0 {
// 		log.Info("[CreateStateReaderFromBlockNumber] Block 0 detected, using ReaderV3 for genesis state")
// 		// ReaderV3 reads from latest state, which should include genesis
// 		reader := state.NewReaderV3(tx)

// 		// Test if genesis state is accessible by trying to read a known contract
// 		testAddr := common.HexToAddress("0x0000000000000000000000000000000000000088")
// 		testCode, _, err := tx.GetLatest(kv.CodeDomain, testAddr[:])
// 		if err != nil {
// 			log.Warn("[CreateStateReaderFromBlockNumber] Failed to test genesis state", "err", err)
// 		} else if len(testCode) == 0 {
// 			log.Warn("[CreateStateReaderFromBlockNumber] Genesis state not found in database - contract code is empty",
// 				"addr", testAddr.Hex(),
// 				"codeLen", len(testCode))
// 		} else {
// 			log.Info("[CreateStateReaderFromBlockNumber] Genesis state found in database",
// 				"addr", testAddr.Hex(),
// 				"codeLen", len(testCode))
// 		}

// 		log.Info("[CreateStateReaderFromBlockNumber] Genesis state reader created", "type", fmt.Sprintf("%T", reader))
// 		return reader, nil
// 	}

// 	if latest {
// 		cacheView, err := stateCache.View(ctx, tx)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return CreateLatestCachedStateReader(cacheView, tx), nil
// 	}
// 	return CreateHistoryStateReader(tx, blockNumber+1, txnIndex, txNumsReader)
// }

func CreateHistoryStateReader(tx kv.TemporalTx, blockNumber uint64,
	txnIndex int, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	r := state.NewHistoryReaderV3()
	r.SetTx(tx)
	//r.SetTrace(true)
	minTxNum, err := txNumsReader.Min(tx, blockNumber)
	if err != nil {
		log.Error("[CreateHistoryStateReader] Failed to get minTxNum", "err", err, "block", blockNumber)
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex + /* 1 system txNum in beginning of block */ 1)

	log.Info("[CreateHistoryStateReader] History state reader",
		"block", blockNumber,
		"minTxNum", minTxNum,
		"txNum", txNum,
		"stateHistoryStartFrom", r.StateHistoryStartFrom())

	if txNum < r.StateHistoryStartFrom() {
		log.Warn("[CreateHistoryStateReader] State pruned",
			"txNum", txNum,
			"stateHistoryStartFrom", r.StateHistoryStartFrom(),
			"block", blockNumber)
		return r, state.PrunedError
	}
	r.SetTxNum(txNum)
	return r, nil
}

func NewLatestStateReader(getter kv.TemporalGetter) state.StateReader {
	return state.NewReaderV3(getter)
}

func NewLatestStateWriter(tx kv.Tx, domains *libstate.SharedDomains, blockReader services.FullBlockReader, blockNum uint64) state.StateWriter {
	minTxNum, err := blockReader.TxnumReader(context.Background()).Min(tx, blockNum)
	if err != nil {
		panic(err)
	}
	txNum := uint64(int(minTxNum) + /* 1 system txNum in beginning of block */ 1)
	domains.SetTxNum(txNum)
	return state.NewWriter(domains.AsPutDel(tx), nil, txNum)
}

func CreateLatestCachedStateReader(cache kvcache.CacheView, tx kv.TemporalTx) state.StateReader {
	return state.NewCachedReader3(cache, tx)
}

// Add this new type and function before CreateStateReaderFromBlockNumber
type genesisStateReader struct {
	state.StateReader
	genesisAlloc types.GenesisAlloc
}

func (r *genesisStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	// First try the base reader
	acc, err := r.StateReader.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	// If account found, return it
	if acc != nil {
		return acc, nil
	}

	// Fall back to genesis alloc
	if alloc, ok := r.genesisAlloc[address]; ok {
		balance, _ := uint256.FromBig(alloc.Balance)
		codeHash := common.Hash{}
		if len(alloc.Code) > 0 {
			codeHash = crypto.Keccak256Hash(alloc.Code)
		}
		return &accounts.Account{
			Nonce:    alloc.Nonce,
			Balance:  *balance,
			CodeHash: codeHash,
			Root:     common.Hash{}, // Empty storage root for genesis
		}, nil
	}

	return nil, nil
}

func (r *genesisStateReader) ReadAccountCode(address common.Address) ([]byte, error) {
	// First try the base reader
	code, err := r.StateReader.ReadAccountCode(address)
	if err != nil {
		return nil, err
	}
	// If code found, return it
	if len(code) > 0 {
		return code, nil
	}

	// Fall back to genesis alloc
	if alloc, ok := r.genesisAlloc[address]; ok {
		log.Info("[genesisStateReader] Reading code from genesis alloc",
			"addr", address.Hex(),
			"codeLen", len(alloc.Code))
		return alloc.Code, nil
	}

	return nil, nil
}

func (r *genesisStateReader) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	// First try the base reader
	val, ok, err := r.StateReader.ReadAccountStorage(address, key)
	if err != nil {
		return uint256.Int{}, false, err
	}
	// If storage found, return it
	if ok {
		return val, true, nil
	}

	// Fall back to genesis alloc
	if alloc, ok := r.genesisAlloc[address]; ok {
		if storageVal, ok := alloc.Storage[key]; ok {
			var result uint256.Int
			result.SetBytes(storageVal.Bytes())
			return result, true, nil
		}
	}

	return uint256.Int{}, false, nil
}

// Update CreateStateReaderFromBlockNumber
func CreateStateReaderFromBlockNumber(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, latest bool, txnIndex int, stateCache kvcache.Cache, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	// // For block 0 (genesis), create a reader that falls back to genesis alloc
	// if blockNumber == 0 {
	// 	// Read genesis alloc from database
	// 	genesis, err := core.ReadGenesis(tx)
	// 	if err != nil {
	// 		return state.NewReaderV3(tx), nil
	// 	}
	// 	if genesis == nil || genesis.Alloc == nil {
	// 		return state.NewReaderV3(tx), nil
	// 	}

	// 	// Create genesis-aware reader
	// 	baseReader := state.NewReaderV3(tx)
	// 	reader := &genesisStateReader{
	// 		StateReader:  baseReader,
	// 		genesisAlloc: genesis.Alloc,
	// 	}

	// 	return reader, nil
	// }

	if latest {
		cacheView, err := stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		return CreateLatestCachedStateReader(cacheView, tx), nil
	}
	return CreateHistoryStateReader(tx, blockNumber+1, txnIndex, txNumsReader)
}
