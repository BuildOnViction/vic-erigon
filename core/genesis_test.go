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

package core_test

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestGenesisBlockHashes(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	check := func(network string) {
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := core.WriteGenesisBlock(tx, genesis, nil, datadir.New(t.TempDir()), logger)
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.Equal(t, block.Hash(), *expect, network)
	}
	for _, network := range networkname.All {
		check(network)
	}
}

func TestGenesisBlockRoots(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	block, _, err := core.GenesisToBlock(core.MainnetGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Hash() != params.MainnetGenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), params.MainnetGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.GnosisGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Root() != params.GnosisGenesisStateRoot {
		t.Errorf("wrong Gnosis Chain genesis state root, got %v, want %v", block.Root(), params.GnosisGenesisStateRoot)
	}
	if block.Hash() != params.GnosisGenesisHash {
		t.Errorf("wrong Gnosis Chain genesis hash, got %v, want %v", block.Hash(), params.GnosisGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.ChiadoGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Root() != params.ChiadoGenesisStateRoot {
		t.Errorf("wrong Chiado genesis state root, got %v, want %v", block.Root(), params.ChiadoGenesisStateRoot)
	}
	if block.Hash() != params.ChiadoGenesisHash {
		t.Errorf("wrong Chiado genesis hash, got %v, want %v", block.Hash(), params.ChiadoGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.TestGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Root() != params.TestGenesisStateRoot {
		t.Errorf("wrong test genesis state root, got %v, want %v", block.Root(), params.TestGenesisStateRoot)
	}
	if block.Hash() != params.TestGenesisHash {
		t.Errorf("wrong test genesis hash, got %v, want %v", block.Hash(), params.TestGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.VictionGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Root() != params.VictionGenesisStateRoot {
		t.Errorf("wrong Viction genesis state root, got %v, want %v", block.Root(), params.VictionGenesisStateRoot)
	}
	if block.Hash() != params.VictionGenesisHash {
		t.Errorf("wrong Viction genesis hash, got %v, want %v", block.Hash(), params.VictionGenesisHash)
	}

	block, _, err = core.GenesisToBlock(core.VictestGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(err)
	if block.Root() != params.VictestGenesisStateRoot {
		t.Errorf("wrong Victest genesis state root, got %v, want %v", block.Root(), params.VictestGenesisStateRoot)
	}
	if block.Hash() != params.VictestGenesisHash {
		t.Errorf("wrong Victest genesis hash, got %v, want %v", block.Hash(), params.VictestGenesisHash)
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	genesis := core.GenesisBlockByChainName(networkname.Mainnet)
	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}

func TestAllocConstructor(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)

	// This deployment code initially sets contract's 0th storage to 0x2a
	// and its 1st storage to 0x01c9.
	deploymentCode := common.FromHex("602a5f556101c960015560048060135f395ff35f355f55")

	funds := big.NewInt(1000000000)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	genSpec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			address: {Constructor: deploymentCode, Balance: funds},
		},
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := mock.MockWithGenesis(t, genSpec, key, false)

	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	//TODO: support historyV3
	reader, err := rpchelper.CreateHistoryStateReader(tx, 1, 0, rawdbv3.TxNums)
	require.NoError(err)
	state := state.New(reader)
	balance, err := state.GetBalance(address)
	require.NoError(err)
	assert.Equal(funds, balance.ToBig())
	code, err := state.GetCode(address)
	require.NoError(err)
	assert.Equal(common.FromHex("5f355f55"), code)

	key0 := common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000")
	storage0 := &uint256.Int{}
	state.GetState(address, key0, storage0)
	assert.Equal(uint256.NewInt(0x2a), storage0)
	key1 := common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001")
	storage1 := &uint256.Int{}
	state.GetState(address, key1, storage1)
	assert.Equal(uint256.NewInt(0x01c9), storage1)
}

// TestPoSVGenesisState tests that PoSV genesis state is written to database and queryable
// First step: Test only contract 0x68 (FoundationMultiSigWallet)
func TestPoSVGenesisState(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)
	logger := log.New()

	// Step 1: Setup - Load Viction genesis (PoSV consensus)
	t.Log("Step 1: Loading Viction genesis")
	genesis := core.VictionGenesisBlock()
	require.NotNil(genesis, "Genesis should not be nil")
	require.NotNil(genesis.Config, "Genesis config should not be nil")
	require.NotNil(genesis.Config.Posv, "Viction should use PoSV consensus")
	require.NotEmpty(genesis.Alloc, "Genesis alloc should not be empty")

	// Step 2: Setup - Create temporal test database
	t.Log("Step 2: Creating temporal test database")
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Step 3: Setup - Commit genesis with database (for PoSV state writing)
	t.Log("Step 3: Committing genesis to database")
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	_, block, err := core.WriteGenesisBlockWithDB(tx, db, genesis, nil, dirs, logger)
	require.NoError(err, "Failed to write genesis block")
	require.NotNil(block, "Genesis block should not be nil")
	require.Equal(uint64(0), block.NumberU64(), "Genesis should be block 0")

	err = tx.Commit()
	require.NoError(err, "Failed to commit genesis transaction")

	// Step 4: Setup - Create read-only transaction and state reader
	t.Log("Step 4: Creating state reader for block 0")
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer roTx.Rollback()

	// Debug: Check raw account data in database for 0x68
	addr68 := common.HexToAddress("0x0000000000000000000000000000000000000068")
	enc, _, err := roTx.GetLatest(kv.AccountsDomain, addr68[:])
	if err == nil && len(enc) > 0 {
		var acc accounts.Account
		if err := accounts.DeserialiseV3(&acc, enc); err == nil {
			t.Logf("DEBUG: Raw account data for 0x68 from DB: balance=%s, nonce=%d, codeHash=%x", acc.Balance.Hex(), acc.Nonce, acc.CodeHash)
		} else {
			t.Logf("DEBUG: Failed to deserialize account data: %v", err)
		}
	} else {
		t.Logf("DEBUG: Account data not found in DB for 0x68: err=%v, len=%d", err, len(enc))
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(context.Background(), roTx, 0, true, 0, nil, rawdbv3.TxNums)
	require.NoError(err, "Failed to create state reader")
	require.NotNil(reader, "State reader should not be nil")

	stateDB := state.New(reader)

	// Step 5: Test Contract 0x68 - FoundationMultiSigWallet
	contractAddr := common.HexToAddress("0x0000000000000000000000000000000000000068")
	t.Logf("Step 5: Testing contract 0x68 (FoundationMultiSigWallet) at %s", contractAddr.Hex())

	// 5.1: Verify contract has code
	t.Log("  5.1: Verifying contract has code")
	code, err := stateDB.GetCode(contractAddr)
	require.NoError(err)
	assert.NotEmpty(code, "Contract 0x68 should have code")
	t.Logf("      Code length: %d bytes", len(code))

	// 5.2: Verify contract has expected balance
	t.Log("  5.2: Verifying contract has expected balance")
	balance, err := stateDB.GetBalance(contractAddr)
	require.NoError(err)
	// Balance from genesis file: 0xd3c21bcecceda10000000 = 16000000000000000000000000
	expectedBalanceBig, _ := big.NewInt(0).SetString("16000000000000000000000000", 10)
	expectedBalance, _ := uint256.FromBig(expectedBalanceBig)
	// Use Cmp() for proper comparison since balance is a value and expectedBalance is a pointer
	if balance.Cmp(expectedBalance) != 0 {
		t.Errorf("Contract 0x68 should have balance %s, but got %s", expectedBalance.Hex(), balance.Hex())
	}
	assert.Equal(0, balance.Cmp(expectedBalance), "Contract 0x68 should have balance 16000000000000000000000000 (0xd3c21bcecceda10000000)")
	t.Logf("      Balance: %s (expected: %s)", balance.Hex(), expectedBalance.Hex())
	if balance.IsZero() {
		t.Errorf("      ERROR: Balance is zero but should be %s", expectedBalance.Hex())
	}

	// 5.3: Verify contract has storage (owners count at slot 3)
	t.Log("  5.3: Verifying contract has storage")
	ownersSlot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")
	storageValue := &uint256.Int{}
	err = stateDB.GetState(contractAddr, ownersSlot, storageValue)
	require.NoError(err)
	assert.False(storageValue.IsZero(), "Contract 0x68 should have owners count in storage slot 3")
	t.Logf("      Storage slot 0x03 (owners count): %s", storageValue.Hex())

	t.Log("Contract 0x68 test completed")
}

func TestPoSVGenesisState0x088(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)
	logger := log.New()

	// Step 1: Setup - Load Viction genesis (PoSV consensus)
	t.Log("Step 1: Loading Viction genesis")
	genesis := core.VictionGenesisBlock()
	require.NotNil(genesis, "Genesis should not be nil")
	require.NotNil(genesis.Config, "Genesis config should not be nil")
	require.NotNil(genesis.Config.Posv, "Viction should use PoSV consensus")
	require.NotEmpty(genesis.Alloc, "Genesis alloc should not be empty")

	// Step 2: Setup - Create temporal test database
	t.Log("Step 2: Creating temporal test database")
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Step 3: Setup - Commit genesis with database (for PoSV state writing)
	t.Log("Step 3: Committing genesis to database")
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	_, block, err := core.WriteGenesisBlockWithDB(tx, db, genesis, nil, dirs, logger)
	require.NoError(err, "Failed to write genesis block")
	require.NotNil(block, "Genesis block should not be nil")
	require.Equal(uint64(0), block.NumberU64(), "Genesis should be block 0")

	err = tx.Commit()
	require.NoError(err, "Failed to commit genesis transaction")

	// Step 4: Setup - Create read-only transaction and state reader
	t.Log("Step 4: Creating state reader for block 0")
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer roTx.Rollback()

	// Step 6: Test Contract 0x88
	contractAddr88 := common.HexToAddress("0x0000000000000000000000000000000000000088")
	t.Logf("Step 6: Testing contract 0x88 at %s", contractAddr88.Hex())

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(context.Background(), roTx, 0, true, 0, nil, rawdbv3.TxNums)
	require.NoError(err, "Failed to create state reader")
	require.NotNil(reader, "State reader should not be nil")

	stateDB := state.New(reader)

	// 6.1: Verify contract has code
	t.Log("  6.1: Verifying contract has code")
	code88, err := stateDB.GetCode(contractAddr88)
	require.NoError(err)
	assert.NotEmpty(code88, "Contract 0x88 should have code")
	t.Logf("      Code length: %d bytes", len(code88))

	// 6.2: Verify contract has expected balance
	t.Log("  6.2: Verifying contract has expected balance")
	balance88, err := stateDB.GetBalance(contractAddr88)
	require.NoError(err)
	// Balance from genesis file: 0x34f086f3b33b68400000
	expectedBalance88Big, _ := big.NewInt(0).SetString("0x34f086f3b33b68400000", 0)
	expectedBalance88Ptr, _ := uint256.FromBig(expectedBalance88Big)
	expectedBalance88 := *expectedBalance88Ptr
	assert.Equal(0, balance88.Cmp(expectedBalance88Ptr), "Contract 0x88 should have balance 0x34f086f3b33b68400000")
	t.Logf("      Balance: %s (expected: %s)", balance88.Hex(), expectedBalance88.Hex())

	// 6.3: Verify contract has storage
	t.Log("  6.3: Verifying contract has storage")
	// Check a few key storage slots from genesis file
	storageSlot3 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")
	storageValue3 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr88, storageSlot3, storageValue3)
	require.NoError(err)
	expectedValue3 := uint256.NewInt(5) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000005"
	assert.Equal(0, storageValue3.Cmp(expectedValue3), "Storage slot 0x03 should be 5")
	t.Logf("      Storage slot 0x03: %s (expected: 5)", storageValue3.Hex())

	storageSlot4 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000004")
	storageValue4 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr88, storageSlot4, storageValue4)
	require.NoError(err)
	expectedValue4 := uint256.NewInt(5) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000005"
	assert.Equal(0, storageValue4.Cmp(expectedValue4), "Storage slot 0x04 should be 5")
	t.Logf("      Storage slot 0x04: %s (expected: 5)", storageValue4.Hex())

	t.Log("Contract 0x88 test completed")
}

func TestPoSVGenesisState0x089(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)
	logger := log.New()

	// Step 1: Setup - Load Viction genesis (PoSV consensus)
	t.Log("Step 1: Loading Viction genesis")
	genesis := core.VictionGenesisBlock()
	require.NotNil(genesis, "Genesis should not be nil")
	require.NotNil(genesis.Config, "Genesis config should not be nil")
	require.NotNil(genesis.Config.Posv, "Viction should use PoSV consensus")
	require.NotEmpty(genesis.Alloc, "Genesis alloc should not be empty")

	// Step 2: Setup - Create temporal test database
	t.Log("Step 2: Creating temporal test database")
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Step 3: Setup - Commit genesis with database (for PoSV state writing)
	t.Log("Step 3: Committing genesis to database")
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	_, block, err := core.WriteGenesisBlockWithDB(tx, db, genesis, nil, dirs, logger)
	require.NoError(err, "Failed to write genesis block")
	require.NotNil(block, "Genesis block should not be nil")
	require.Equal(uint64(0), block.NumberU64(), "Genesis should be block 0")

	err = tx.Commit()
	require.NoError(err, "Failed to commit genesis transaction")

	// Step 4: Setup - Create read-only transaction and state reader
	t.Log("Step 4: Creating state reader for block 0")
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer roTx.Rollback()

	// Step 5: Test Contract 0x89
	contractAddr89 := common.HexToAddress("0x0000000000000000000000000000000000000089")
	t.Logf("Step 5: Testing contract 0x89 at %s", contractAddr89.Hex())

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(context.Background(), roTx, 0, true, 0, nil, rawdbv3.TxNums)
	require.NoError(err, "Failed to create state reader")
	require.NotNil(reader, "State reader should not be nil")

	stateDB := state.New(reader)

	// 5.1: Verify contract has code
	t.Log("  5.1: Verifying contract has code")
	code89, err := stateDB.GetCode(contractAddr89)
	require.NoError(err)
	assert.NotEmpty(code89, "Contract 0x89 should have code")
	t.Logf("      Code length: %d bytes", len(code89))

	// 5.2: Verify contract has expected balance (should be 0)
	t.Log("  5.2: Verifying contract has expected balance")
	balance89, err := stateDB.GetBalance(contractAddr89)
	require.NoError(err)
	// Balance from genesis file: 0x0
	assert.True(balance89.IsZero(), "Contract 0x89 should have balance 0")
	t.Logf("      Balance: %s (expected: 0)", balance89.Hex())

	// 5.3: Verify contract has storage
	t.Log("  5.3: Verifying contract has storage")
	// Check storage slot 0x02 from genesis file
	storageSlot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	storageValue2 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr89, storageSlot2, storageValue2)
	require.NoError(err)
	expectedValue2 := uint256.NewInt(0x384) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000384" = 900
	assert.Equal(0, storageValue2.Cmp(expectedValue2), "Storage slot 0x02 should be 0x384 (900)")
	t.Logf("      Storage slot 0x02: %s (expected: 0x384 / 900)", storageValue2.Hex())

	t.Log("Contract 0x89 test completed")
}

func TestPoSVGenesisState0x090(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)
	logger := log.New()

	// Step 1: Setup - Load Viction genesis (PoSV consensus)
	t.Log("Step 1: Loading Viction genesis")
	genesis := core.VictionGenesisBlock()
	require.NotNil(genesis, "Genesis should not be nil")
	require.NotNil(genesis.Config, "Genesis config should not be nil")
	require.NotNil(genesis.Config.Posv, "Viction should use PoSV consensus")
	require.NotEmpty(genesis.Alloc, "Genesis alloc should not be empty")

	// Step 2: Setup - Create temporal test database
	t.Log("Step 2: Creating temporal test database")
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Step 3: Setup - Commit genesis with database (for PoSV state writing)
	t.Log("Step 3: Committing genesis to database")
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	_, block, err := core.WriteGenesisBlockWithDB(tx, db, genesis, nil, dirs, logger)
	require.NoError(err, "Failed to write genesis block")
	require.NotNil(block, "Genesis block should not be nil")
	require.Equal(uint64(0), block.NumberU64(), "Genesis should be block 0")

	err = tx.Commit()
	require.NoError(err, "Failed to commit genesis transaction")

	// Step 4: Setup - Create read-only transaction and state reader
	t.Log("Step 4: Creating state reader for block 0")
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer roTx.Rollback()

	// Step 5: Test Contract 0x90
	contractAddr90 := common.HexToAddress("0x0000000000000000000000000000000000000090")
	t.Logf("Step 5: Testing contract 0x90 at %s", contractAddr90.Hex())

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(context.Background(), roTx, 0, true, 0, nil, rawdbv3.TxNums)
	require.NoError(err, "Failed to create state reader")
	require.NotNil(reader, "State reader should not be nil")

	stateDB := state.New(reader)

	// 5.1: Verify contract has code
	t.Log("  5.1: Verifying contract has code")
	code90, err := stateDB.GetCode(contractAddr90)
	require.NoError(err)
	assert.NotEmpty(code90, "Contract 0x90 should have code")
	t.Logf("      Code length: %d bytes", len(code90))

	// 5.2: Verify contract has expected balance (should be 0)
	t.Log("  5.2: Verifying contract has expected balance")
	balance90, err := stateDB.GetBalance(contractAddr90)
	require.NoError(err)
	// Balance from genesis file: 0x0
	assert.True(balance90.IsZero(), "Contract 0x90 should have balance 0")
	t.Logf("      Balance: %s (expected: 0)", balance90.Hex())

	// 5.3: Verify contract has no storage (genesis file shows no storage entries)
	t.Log("  5.3: Verifying contract has no storage")
	// Check that a random storage slot is empty (since genesis file has no storage for 0x90)
	storageSlot0 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
	storageValue0 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr90, storageSlot0, storageValue0)
	require.NoError(err)
	assert.True(storageValue0.IsZero(), "Storage slot 0x00 should be empty (0) for contract 0x90")
	t.Logf("      Storage slot 0x00: %s (expected: 0 - no storage in genesis)", storageValue0.Hex())

	t.Log("Contract 0x90 test completed")
}

func TestPoSVGenesisState0x099(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	assert := assert.New(t)
	logger := log.New()

	// Step 1: Setup - Load Viction genesis (PoSV consensus)
	t.Log("Step 1: Loading Viction genesis")
	genesis := core.VictionGenesisBlock()
	require.NotNil(genesis, "Genesis should not be nil")
	require.NotNil(genesis.Config, "Genesis config should not be nil")
	require.NotNil(genesis.Config.Posv, "Viction should use PoSV consensus")
	require.NotEmpty(genesis.Alloc, "Genesis alloc should not be empty")

	// Step 2: Setup - Create temporal test database
	t.Log("Step 2: Creating temporal test database")
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Step 3: Setup - Commit genesis with database (for PoSV state writing)
	t.Log("Step 3: Committing genesis to database")
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	_, block, err := core.WriteGenesisBlockWithDB(tx, db, genesis, nil, dirs, logger)
	require.NoError(err, "Failed to write genesis block")
	require.NotNil(block, "Genesis block should not be nil")
	require.Equal(uint64(0), block.NumberU64(), "Genesis should be block 0")

	err = tx.Commit()
	require.NoError(err, "Failed to commit genesis transaction")

	// Step 4: Setup - Create read-only transaction and state reader
	t.Log("Step 4: Creating state reader for block 0")
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(err)
	defer roTx.Rollback()

	// Step 5: Test Contract 0x99
	contractAddr99 := common.HexToAddress("0x0000000000000000000000000000000000000099")
	t.Logf("Step 5: Testing contract 0x99 at %s", contractAddr99.Hex())

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(context.Background(), roTx, 0, true, 0, nil, rawdbv3.TxNums)
	require.NoError(err, "Failed to create state reader")
	require.NotNil(reader, "State reader should not be nil")

	stateDB := state.New(reader)

	// 5.1: Verify contract has code
	t.Log("  5.1: Verifying contract has code")
	code99, err := stateDB.GetCode(contractAddr99)
	require.NoError(err)
	assert.NotEmpty(code99, "Contract 0x99 should have code")
	t.Logf("      Code length: %d bytes", len(code99))

	// 5.2: Verify contract has expected balance
	t.Log("  5.2: Verifying contract has expected balance")
	balance99, err := stateDB.GetBalance(contractAddr99)
	require.NoError(err)
	// Balance from genesis file: 0x9b828c6bde7e823c00000
	expectedBalance99Big, _ := big.NewInt(0).SetString("0x9b828c6bde7e823c00000", 0)
	expectedBalance99Ptr, _ := uint256.FromBig(expectedBalance99Big)
	assert.Equal(0, balance99.Cmp(expectedBalance99Ptr), "Contract 0x99 should have balance 0x9b828c6bde7e823c00000")
	t.Logf("      Balance: %s (expected: %s)", balance99.Hex(), expectedBalance99Ptr.Hex())

	// 5.3: Verify contract has storage
	t.Log("  5.3: Verifying contract has storage")
	// Check storage slot 0x03
	storageSlot3 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")
	storageValue3 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr99, storageSlot3, storageValue3)
	require.NoError(err)
	expectedValue3 := uint256.NewInt(3) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000003"
	assert.Equal(0, storageValue3.Cmp(expectedValue3), "Storage slot 0x03 should be 3")
	t.Logf("      Storage slot 0x03: %s (expected: 3)", storageValue3.Hex())

	// Check storage slot 0x04
	storageSlot4 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000004")
	storageValue4 := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr99, storageSlot4, storageValue4)
	require.NoError(err)
	expectedValue4 := uint256.NewInt(2) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000002"
	assert.Equal(0, storageValue4.Cmp(expectedValue4), "Storage slot 0x04 should be 2")
	t.Logf("      Storage slot 0x04: %s (expected: 2)", storageValue4.Hex())

	// Check storage slot with hash key (0x08bd426a285149bb83095f9f98d8f4cfafbf4df11f3590e7b232ac9de1102399)
	storageSlotHash := common.HexToHash("0x08bd426a285149bb83095f9f98d8f4cfafbf4df11f3590e7b232ac9de1102399")
	storageValueHash := uint256.NewInt(0)
	err = stateDB.GetState(contractAddr99, storageSlotHash, storageValueHash)
	require.NoError(err)
	expectedValueHash := uint256.NewInt(1) // From genesis: "0x0000000000000000000000000000000000000000000000000000000000000001"
	assert.Equal(0, storageValueHash.Cmp(expectedValueHash), "Storage slot 0x08bd426a285149bb83095f9f98d8f4cfafbf4df11f3590e7b232ac9de1102399 should be 1")
	t.Logf("      Storage slot 0x08bd426a...: %s (expected: 1)", storageValueHash.Hex())

	t.Log("Contract 0x99 test completed")
}

// See https://github.com/erigontech/erigon/pull/11264
func TestDecodeBalance0(t *testing.T) {
	genesisData, err := os.ReadFile("./genesis_test.json")
	require.NoError(t, err)

	genesis := &types.Genesis{}
	err = json.Unmarshal(genesisData, genesis)
	require.NoError(t, err)
	_ = genesisData
}
