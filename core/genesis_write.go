// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package core

import (
	"context"
	"crypto/ecdsa"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	params2 "github.com/erigontech/erigon/params"
)

//go:embed allocs
var allocs embed.FS

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	config := params2.ChainConfigByGenesisHash(e.Stored)
	if config == nil {
		return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
	}
	return fmt.Sprintf("database contains incompatible genesis (try with --chain=%s)", config.ChainName)
}

// CommitGenesisBlock writes or updates the genesis block in db.
// The block that will be used is:
//
//	                     genesis == nil       genesis != nil
//	                  +------------------------------------------
//	db has no genesis |  main-net          |  genesis
//	db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func CommitGenesisBlock(db kv.RwDB, genesis *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	return CommitGenesisBlockWithOverride(db, genesis, nil, dirs, logger)
}

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *types.Genesis, overrideOsakaTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	// For PoSV consensus, we need a temporal transaction to write genesis state
	if genesis != nil && genesis.Config != nil && (genesis.Config.Consensus == chain.PosvConsensus || genesis.Config.Posv != nil) {
		// Set up aggregator and temporal DB for PoSV
		salt, err := state2.GetStateIndicesSalt(dirs, false, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get state salt: %w", err)
		}

		agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create aggregator: %w", err)
		}
		defer agg.Close()

		tdb, err := temporal.New(db, agg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create temporal DB: %w", err)
		}
		defer tdb.Close()

		tx, err := tdb.BeginTemporalRw(context.Background())
		if err != nil {
			return nil, nil, err
		}
		defer tx.Rollback()

		c, b, err := WriteGenesisBlock(tx, genesis, overrideOsakaTime, dirs, logger)
		if err != nil {
			return c, b, err
		}
		err = tx.Commit()
		if err != nil {
			return c, b, err
		}
		return c, b, nil
	}

	// For non-PoSV, use regular transaction
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overrideOsakaTime, dirs, logger)
	if err != nil {
		return c, b, err
	}
	err = tx.Commit()
	if err != nil {
		return c, b, err
	}
	return c, b, nil
}

func configOrDefault(g *types.Genesis, genesisHash common.Hash) *chain.Config {
	if g != nil {
		return g.Config
	}

	config := params2.ChainConfigByGenesisHash(genesisHash)
	if config != nil {
		return config
	} else {
		return chain.AllProtocolChanges
	}
}

func WriteGenesisBlock(tx kv.RwTx, genesis *types.Genesis, overrideOsakaTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	return WriteGenesisBlockWithDB(tx, nil, genesis, overrideOsakaTime, dirs, logger)
}

func WriteGenesisBlockWithDB(tx kv.RwTx, db kv.RwDB, genesis *types.Genesis, overrideOsakaTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	if err := WriteGenesisIfNotExist(tx, genesis); err != nil {
		return nil, nil, err
	}

	var storedBlock *types.Block
	if genesis != nil && genesis.Config == nil {
		return chain.AllProtocolChanges, nil, types.ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(tx, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *chain.Config) {
		if overrideOsakaTime != nil {
			config.OsakaTime = overrideOsakaTime
		}
	}

	if (storedHash == common.Hash{}) {
		custom := true
		if genesis == nil {
			logger.Info("Using custom genesis config for network 1337")
			genesis = loadCustomGenesis()
			custom = true
		}
		applyOverrides(genesis.Config)
		block, _, err1 := write(tx, db, genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		if custom {
			logger.Info("Writing custom genesis block", "hash", block.Hash())
		}
		return genesis.Config, block, nil
	}

	// Genesis block already exists, check if we need to use custom genesis for network 1337
	if genesis == nil {
		// Force custom genesis for network 1337
		logger.Info("Using custom genesis config for network 1337")
		genesis = loadCustomGenesis()
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		block, _, err1 := GenesisToBlock(genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		hash := block.Hash()
		if hash != storedHash {
			return genesis.Config, block, &GenesisMismatchError{Stored: storedHash, New: hash}
		}
	}
	number := rawdb.ReadHeaderNumber(tx, storedHash)
	if number != nil {
		var err error
		storedBlock, _, err = rawdb.ReadBlockWithSenders(tx, storedHash, *number)
		if err != nil {
			return genesis.Config, nil, err
		}
	}
	// Get the existing chain configuration.
	newCfg := configOrDefault(genesis, storedHash)
	applyOverrides(newCfg)
	if err := newCfg.CheckConfigForkOrder(); err != nil {
		return newCfg, nil, err
	}
	storedCfg, storedErr := ReadChainConfig(tx, storedHash)
	if storedErr != nil && newCfg.Bor == nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		logger.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(tx, storedHash, newCfg)
		if err1 != nil {
			return newCfg, nil, err1
		}
		return newCfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a private chain if no new
	// config is supplied. This is useful, for example, to preserve DB config created by erigon init.
	// In that case, only apply the overrides.
	if genesis == nil && params2.ChainConfigByGenesisHash(storedHash) == nil {
		newCfg = storedCfg
		applyOverrides(newCfg)
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadHeaderHash(tx))
	if height != nil {
		compatibilityErr := storedCfg.CheckCompatible(newCfg, *height)
		if compatibilityErr != nil && *height != 0 && compatibilityErr.RewindTo != 0 {
			return newCfg, storedBlock, compatibilityErr
		}
	}
	if err := rawdb.WriteChainConfig(tx, storedHash, newCfg); err != nil {
		return newCfg, nil, err
	}
	return newCfg, storedBlock, nil
}

func WriteGenesisState(g *types.Genesis, tx kv.RwTx, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	return WriteGenesisStateWithDB(g, tx, nil, dirs, logger)
}

func WriteGenesisStateWithDB(g *types.Genesis, tx kv.RwTx, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := GenesisToBlock(g, dirs, logger)
	fmt.Println("-> block", block.Hash())
	if err != nil {
		return nil, nil, err
	}

	if block.Number().Sign() != 0 {
		return nil, statedb, errors.New("can't commit genesis block with number > 0")
	}

	// For PoSV consensus, write genesis state to database
	// Other consensus engines don't need genesis state persisted
	if g.Config != nil && (g.Config.Consensus == chain.PosvConsensus || g.Config.Posv != nil) {
		fmt.Println("-> Writing genesis state to database for PoSV consensus")
		logger.Info("Writing genesis state to database for PoSV consensus")

		// Get state root from computed block
		stateRoot := block.Root()

		// Check if tx is already a temporal transaction
		var temporalTx kv.TemporalRwTx
		createdNewTx := false
		if tempTx, ok := tx.(kv.TemporalRwTx); ok {
			// Already a temporal transaction, use it directly
			temporalTx = tempTx
		} else if db != nil {
			// Transaction is not temporal, but we have the database - create temporal transaction
			salt, err := state2.GetStateIndicesSalt(dirs, false, logger)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get state salt: %w", err)
			}

			agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create aggregator: %w", err)
			}
			defer agg.Close()

			tdb, err := temporal.New(db, agg)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create temporal DB: %w", err)
			}
			defer tdb.Close()

			// Create a new temporal transaction
			// Note: We need to commit the original tx first, then use the temporal one
			// But we can't do that here. Instead, we'll use the temporal tx for state writing
			// and the original tx for block writing
			tempTx, err := tdb.BeginTemporalRw(context.Background())
			if err != nil {
				return nil, nil, fmt.Errorf("failed to begin temporal tx: %w", err)
			}
			defer tempTx.Rollback()
			temporalTx = tempTx
			createdNewTx = true
		} else {
			// Transaction is not temporal and we don't have the database
			return nil, nil, fmt.Errorf("transaction must be a temporal transaction (kv.TemporalRwTx) or database must be provided to write PoSV genesis state")
		}

		sd, err := state2.NewSharedDomains(temporalTx, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create shared domains: %w", err)
		}
		defer sd.Close()

		blockNum := uint64(0)
		txNum := uint64(1)

		// Create state reader and writer
		r := state.NewReaderV3(sd.AsGetter(temporalTx))
		w := state.NewWriter(sd.AsPutDel(temporalTx), nil, txNum)
		genesisStatedb := state.New(r)
		genesisStatedb.SetTrace(false)

		// Write all genesis accounts to the database
		keys := sortedAllocKeys(g.Alloc)
		for _, key := range keys {
			addr := common.BytesToAddress([]byte(key))
			account := g.Alloc[addr]
			balance, overflow := uint256.FromBig(account.Balance)
			fmt.Println("-> addr", addr.Hex(), "balance", balance.Hex())
			if overflow {
				return nil, nil, fmt.Errorf("balance overflow for address %x", addr)
			}
			// For accounts with code/storage, explicitly create them to ensure they are written
			hasCodeOrStorage := len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0
			if hasCodeOrStorage {
				// Create account explicitly for contracts to ensure createdContract flag is set
				if err := genesisStatedb.CreateAccount(addr, true); err != nil {
					return nil, nil, fmt.Errorf("failed to create contract account for %x: %w", addr, err)
				}
			} else {
				// For regular accounts, just get or create
				_, err := genesisStatedb.GetOrNewStateObject(addr)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to get or create state object for %x: %w", addr, err)
				}
			}

			// Set code first (if any)
			if len(account.Code) > 0 {
				genesisStatedb.SetCode(addr, account.Code)
			}
			if account.Nonce > 0 {
				genesisStatedb.SetNonce(addr, account.Nonce)
			}

			// Set balance BEFORE storage and incarnation (matching MakePreState order)
			if !balance.IsZero() {
				if err := genesisStatedb.SetBalance(addr, *balance, tracing.BalanceIncreaseGenesisBalance); err != nil {
					return nil, nil, fmt.Errorf("failed to set balance for %x: %w", addr, err)
				}
			}

			// Set storage (if any)
			for key, value := range account.Storage {
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				genesisStatedb.SetState(addr, key, *val)
			}

			// Handle constructor (if any)
			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, g.Config, genesisStatedb, block.Header()); err != nil {
					return nil, nil, fmt.Errorf("failed to create contract for %x: %w", addr, err)
				}
			}

			// Set incarnation for contracts LAST (after balance is set)
			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				genesisStatedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}

			// Debug: Verify balance is still set correctly after all operations
			if !balance.IsZero() {
				actualBalance, err := genesisStatedb.GetBalance(addr)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to get balance for %x: %w", addr, err)
				}
				if actualBalance.Cmp(balance) != 0 {
					fmt.Printf("WARNING: Balance mismatch for %x after all ops: expected %s, got %s\n", addr, balance.Hex(), actualBalance.Hex())
				} else {
					fmt.Printf("OK: Balance set correctly for %x: %s\n", addr, actualBalance.Hex())
				}
			}
		}

		// Debug: Check if account 0x68 is marked as dirty before FinalizeTx
		addr68 := common.HexToAddress("0x0000000000000000000000000000000000000068")
		// Access the state object directly to check its state
		// We can't access it directly, but we can verify the balance is still set
		balance68Before, _ := genesisStatedb.GetBalance(addr68)
		fmt.Printf("DEBUG: Balance for 0x68 before FinalizeTx: %s\n", balance68Before.Hex())

		// Finalize and commit the state
		if err = genesisStatedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return nil, nil, fmt.Errorf("failed to finalize tx: %w", err)
		}

		// Debug: Check balance after FinalizeTx
		balance68After, _ := genesisStatedb.GetBalance(addr68)
		fmt.Printf("DEBUG: Balance for 0x68 after FinalizeTx: %s\n", balance68After.Hex())

		if err = genesisStatedb.CommitBlock(&chain.Rules{}, w); err != nil {
			return nil, nil, fmt.Errorf("failed to commit block: %w", err)
		}

		// Debug: Try to read account directly from database after commit
		// Use SharedDomains.GetLatest instead of temporalTx.GetLatest to see writes in cache
		if temporalTx != nil && sd != nil {
			enc, _, err := sd.GetLatest(kv.AccountsDomain, temporalTx, addr68[:])
			if err == nil {
				if len(enc) > 0 {
					var acc accounts.Account
					if err := accounts.DeserialiseV3(&acc, enc); err == nil {
						fmt.Printf("DEBUG: Account 0x68 in DB after CommitBlock (via SharedDomains): balance=%s, nonce=%d, codeHash=%x\n", acc.Balance.Hex(), acc.Nonce, acc.CodeHash)
					} else {
						fmt.Printf("DEBUG: Failed to deserialize account 0x68: %v\n", err)
					}
				} else {
					fmt.Printf("DEBUG: Account 0x68 not found in DB after CommitBlock (len=0, via SharedDomains)\n")
				}
			} else {
				fmt.Printf("DEBUG: Error reading account 0x68 from DB (via SharedDomains): %v\n", err)
			}

			// Also try direct temporalTx.GetLatest for comparison
			enc2, _, err2 := temporalTx.GetLatest(kv.AccountsDomain, addr68[:])
			if err2 == nil {
				if len(enc2) > 0 {
					var acc accounts.Account
					if err := accounts.DeserialiseV3(&acc, enc2); err == nil {
						fmt.Printf("DEBUG: Account 0x68 in DB after CommitBlock (via temporalTx): balance=%s, nonce=%d, codeHash=%x\n", acc.Balance.Hex(), acc.Nonce, acc.CodeHash)
					}
				} else {
					fmt.Printf("DEBUG: Account 0x68 not found via temporalTx.GetLatest (len=0)\n")
				}
			}
		}

		// Flush SharedDomains to make writes visible in the database
		// This is necessary because DomainPut writes to buffered writers that need to be flushed
		if err = sd.FlushWithoutCommitment(context.Background(), temporalTx.(kv.RwTx)); err != nil {
			return nil, nil, fmt.Errorf("failed to flush shared domains: %w", err)
		}

		// Compute commitment
		rh, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "genesis")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to compute commitment: %w", err)
		}

		// Verify the root matches
		computedRoot := common.BytesToHash(rh)
		if computedRoot != stateRoot {
			return nil, nil, fmt.Errorf("state root mismatch: computed %x, expected %x", computedRoot, stateRoot)
		}

		// Commit temporal transaction if we created it (not if it was passed in)
		if createdNewTx {
			if err = temporalTx.Commit(); err != nil {
				return nil, nil, fmt.Errorf("failed to commit genesis tx: %w", err)
			}
		}

		logger.Info("Genesis state written to database for PoSV",
			"root", stateRoot.Hex(),
			"accounts", len(g.Alloc))

		return block, genesisStatedb, nil
	}

	// For non-PoSV consensus, use NoopWriter (don't persist state)
	stateWriter := state.NewNoopWriter()
	if err := statedb.CommitBlock(&chain.Rules{}, stateWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %w", err)
	}

	return block, statedb, nil
}

func MustCommitGenesis(g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _, err := write(tx, db, g, dirs, logger)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}

// Write writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func write(tx kv.RwTx, db kv.RwDB, g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := WriteGenesisStateWithDB(g, tx, db, dirs, logger)
	if err != nil {
		return block, statedb, err
	}
	err = rawdb.WriteGenesisBesideState(block, tx, g)
	return block, statedb, err
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db kv.RwDB, addr common.Address, balance *big.Int, dirs datadir.Dirs, logger log.Logger) *types.Block {
	g := types.Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: chain.TestChainConfig}
	block := MustCommitGenesis(&g, db, dirs, logger)
	return block
}

type GenAccount struct {
	Addr    common.Address
	Balance *big.Int
}

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      readPrealloc("allocs/mainnet.json"),
	}
}

// HoleskyGenesisBlock returns the Holesky main net genesis block.
func HoleskyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.HoleskyChainConfig,
		Nonce:      4660,
		GasLimit:   25000000,
		Difficulty: big.NewInt(1),
		Timestamp:  1695902100,
		Alloc:      readPrealloc("allocs/holesky.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      readPrealloc("allocs/sepolia.json"),
	}
}

// HoodiGenesisBlock returns the Hoodi network genesis block.
func HoodiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.HoodiChainConfig,
		Nonce:      0x1234,
		ExtraData:  []byte(""),
		GasLimit:   0x2255100, // 36M
		Difficulty: big.NewInt(1),
		Timestamp:  1742212800,
		Alloc:      readPrealloc("allocs/hoodi.json"),
	}
}

// AmoyGenesisBlock returns the Amoy network genesis block.
func AmoyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.AmoyChainConfig,
		Nonce:      0,
		Timestamp:  1700225065,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/amoy.json"),
	}
}

// BorMainnetGenesisBlock returns the Bor Mainnet network genesis block.
func BorMainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.BorMainnetChainConfig,
		Nonce:      0,
		Timestamp:  1590824836,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_mainnet.json"),
	}
}

func BorDevnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.BorDevnetChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_devnet.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/gnosis.json"),
	}
}

func VictionGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.VictionChainConfig,
		Nonce:      0,
		Timestamp:  0x5c1358f5,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001b82c4bf317fcafe3d77e8b444c82715d216afe845b7bd987fa22c9bac89b71f0ded03f6e150ba31ad670b2b166684657ffff95f4810380ae7381e9bce41231d5dd8cdd7499e418b648c00af75d184a2f9aba09a6fa4a46fb1a6a3919b027d9cac5aa6890000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x47b760,
		Difficulty: big.NewInt(0x1),
		Alloc:      readPrealloc("allocs/viction.json"),
	}
}

func VictestGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.VictestChainConfig,
		Nonce:      0,
		Timestamp:  0x65309e07,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001acc82e4cafc08af311852da4722fb34529322c91e7c9fae96ec2efb129b69ff5e0e8a8b8acb6add4f4b5983cdf8f674fa63de933713f245502f97676fdef2bd0d35de1c72016cfbbf2a6f2c59b8c2977e40b530a68d1dd71b7941cfb53534c3806aa5180000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x47b760,
		Difficulty: big.NewInt(0x1),
		Alloc:      readPrealloc("allocs/victest.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.ChiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/chiado.json"),
	}
}
func TestGenesisBlock() *types.Genesis {
	return &types.Genesis{Config: chain.TestChainConfig}
}

// Pre-calculated version of:
//
//	DevnetSignPrivateKey = crypto.HexToECDSA(sha256.Sum256([]byte("erigon devnet key")))
//	DevnetEtherbase=crypto.PubkeyToAddress(DevnetSignPrivateKey.PublicKey)
var DevnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
var DevnetEtherbase = common.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

// DevnetSignKey is defined like this to allow the devnet process to pre-allocate keys
// for nodes and then pass the address via --miner.etherbase - the function will be called
// to retieve the mining key
var DevnetSignKey = func(address common.Address) *ecdsa.PrivateKey {
	return DevnetSignPrivateKey
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet common.Address) *types.Genesis {
	// Override the default period to the user requested one
	var config chain.Config
	copier.Copy(&config, params2.AllCliqueProtocolChanges)
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &types.Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/dev.json"),
	}
}

// GenesisToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func GenesisToBlock(g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}
	if g.Alloc == nil {
		g.Alloc = make(types.GenesisAlloc)
	}

	head, withdrawals := rawdb.GenesisWithoutStateToBlock(g)

	var root common.Hash
	var statedb *state.IntraBlockState // reader behind this statedb is dead at the moment of return, tx is rolled back

	ctx := context.Background()
	wg, ctx := errgroup.WithContext(ctx)
	// we may run inside write tx, can't open 2nd write tx in same goroutine
	wg.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v, %s", rec, dbg.Stack())
			}
		}()
		// some users creating > 1Gb custome genesis by `erigon init`
		genesisTmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(dirs.DataDir).MapSize(2 * datasize.GB).GrowthStep(1 * datasize.MB).MustOpen()
		defer genesisTmpDB.Close()

		salt, err := state2.GetStateIndicesSalt(dirs, false, logger)
		if err != nil {
			return err
		}
		agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, genesisTmpDB, logger)
		if err != nil {
			return err
		}
		defer agg.Close()

		tdb, err := temporal.New(genesisTmpDB, agg)
		if err != nil {
			return err
		}
		defer tdb.Close()

		tx, err := tdb.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		sd, err := state2.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer sd.Close()

		blockNum := uint64(0)
		txNum := uint64(1) //2 system txs in begin/end of block. Attribute state-writes to first, consensus state-changes to second

		//r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
		statedb = state.New(r)
		statedb.SetTrace(false)

		hasConstructorAllocation := false
		for _, account := range g.Alloc {
			if len(account.Constructor) > 0 {
				hasConstructorAllocation = true
				break
			}
		}
		// See https://github.com/NethermindEth/nethermind/blob/master/src/Nethermind/Nethermind.Consensus.AuRa/InitializationSteps/LoadGenesisBlockAuRa.cs
		if hasConstructorAllocation && g.Config.Aura != nil {
			statedb.CreateAccount(common.Address{}, false)
		}

		keys := sortedAllocKeys(g.Alloc)
		for _, key := range keys {
			addr := common.BytesToAddress([]byte(key))
			account := g.Alloc[addr]

			balance, overflow := uint256.FromBig(account.Balance)
			if overflow {
				panic("overflow at genesis allocs")
			}
			statedb.AddBalance(addr, *balance, tracing.BalanceIncreaseGenesisBalance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)
			for key, value := range account.Storage {
				key := key
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				statedb.SetState(addr, key, *val)
			}

			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, g.Config, statedb, head); err != nil {
					return err
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}
		}
		if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return err
		}

		rh, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "genesis")
		if err != nil {
			return err
		}
		root = common.BytesToHash(rh)
		return nil
	})

	if err := wg.Wait(); err != nil {
		return nil, nil, err
	}

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, withdrawals), statedb, nil
}

func sortedAllocKeys(m types.GenesisAlloc) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = string(k.Bytes())
		i++
	}
	slices.Sort(keys)
	return keys
}

func readPrealloc(filename string) types.GenesisAlloc {
	f, err := allocs.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open genesis preallocation for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	ga := make(types.GenesisAlloc)
	err = decoder.Decode(&ga)
	if err != nil {
		panic(fmt.Sprintf("Could not parse genesis preallocation for %s: %v", filename, err))
	}
	return ga
}

func GenesisBlockByChainName(chain string) *types.Genesis {
	switch chain {
	case networkname.Mainnet:
		return MainnetGenesisBlock()
	case networkname.Holesky:
		return HoleskyGenesisBlock()
	case networkname.Sepolia:
		return SepoliaGenesisBlock()
	case networkname.Hoodi:
		return HoodiGenesisBlock()
	case networkname.Amoy:
		return AmoyGenesisBlock()
	case networkname.BorMainnet:
		return BorMainnetGenesisBlock()
	case networkname.BorDevnet:
		return BorDevnetGenesisBlock()
	case networkname.Gnosis:
		return GnosisGenesisBlock()
	case networkname.Viction:
		return VictionGenesisBlock()
	case networkname.Victest:
		return VictestGenesisBlock()
	case networkname.Chiado:
		return ChiadoGenesisBlock()
	case networkname.Test:
		return TestGenesisBlock()
	default:
		return nil
	}
}

func loadCustomGenesis() *types.Genesis {
	alloc := make(types.GenesisAlloc)
	balance, _ := big.NewInt(0).SetString("1000000000000000000000", 10)
	alloc[common.HexToAddress("0xaa49A1336Ad98B59Aff3f20184c97c48ac524A98")] = types.GenesisAccount{
		Balance: balance,
	}
	alloc[common.HexToAddress("0x9f2A789B0831AC55eCe9815d8371040a293c4306")] = types.GenesisAccount{
		Balance: balance,
	}

	return &types.Genesis{
		Config: &chain.Config{
			ChainID:               big.NewInt(1337),
			HomesteadBlock:        big.NewInt(0),
			TangerineWhistleBlock: big.NewInt(0),
			EIP155Block:           big.NewInt(0),
			Clique: &chain.CliqueConfig{
				Period: 5,
				Epoch:  30000,
			},
		},
		Alloc:      alloc,
		Nonce:      0x42,
		Timestamp:  0,
		Difficulty: big.NewInt(0),
		GasLimit:   30000000,
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		ExtraData:  common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000aa49A1336Ad98B59Aff3f20184c97c48ac524A98000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	}
}
