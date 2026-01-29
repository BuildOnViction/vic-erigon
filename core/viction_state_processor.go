package core

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/holiman/uint256"
)

var (
	ErrStopPreparingBlock = errors.New("stop preparing block")
)

// CheckVictionBlacklist checks if the sender or receiver is in the blacklist
func CheckVictionBlacklist(config *chain.Config, header *types.Header, tx types.Transaction) error {
	// check black-list txs after hf
	if config.IsTIPBlacklist(header.Number.Uint64()) {
		sender, _ := tx.Sender(*types.MakeSigner(config, header.Number.Uint64(), header.Time))
		// check if sender is in black list
		if chain.IsBlacklisted(sender) {
			return fmt.Errorf("block contains transaction with sender in black-list: %v", sender.Hex())
		}
		// check if receiver is in black list
		if tx.GetTo() != nil && chain.IsBlacklisted(*tx.GetTo()) {
			return fmt.Errorf("block contains transaction with receiver in black-list: %v", tx.GetTo().Hex())
		}
	}
	return nil
}

// ValidateVictionTx performs TomoZ and TomoX validations
func ValidateVictionTx(config *chain.Config, getBlock func(common.Hash, uint64) *types.Block, ibs *state.IntraBlockState, header *types.Header, tx types.Transaction) error {
	blockNum := header.Number.Uint64()

	// TODO: Port IsTomoZEnabled and IsTomoXEnabled from chain config or implement logic
	// validate balance slot, minFee slot for TomoZ
	if config.IsTIPTomoX(blockNum) { // Assuming TomoX implies TomoZ or similar logic
		// if tx.IsTomoZApplyTransaction() { ... }
	}

	// validate balance slot, token decimal for TomoX
	if config.IsTIPTomoX(blockNum) {
		// if tx.IsTomoXApplyTransaction() { ... }
	}

	return nil
}

// ApplyVictionSpecialTx applies Viction specific transactions (BlockSign, Trading, etc)
// Returns the receipt if handled, nil otherwise.
func ApplyVictionSpecialTx(
	config *chain.Config,
	ibs *state.IntraBlockState,
	header *types.Header,
	tx types.Transaction,
	usedGas *uint64,
) (*types.Receipt, error) {
	to := tx.GetTo()
	if to != nil && *to == config.Viction.ValidatorBlockSignContract && config.IsTIPSigning(header.Number.Uint64()) {
		return ApplySignTransaction(config, ibs, header, tx, usedGas)
	}

	if to != nil && *to == config.Viction.TomoXContract && config.IsTIPTomoX(header.Number.Uint64()) {
		return ApplyEmptyTransaction(config, ibs, header, tx, usedGas)
	}
	if to != nil && *to == config.Viction.LendingContract && config.IsTIPTomoX(header.Number.Uint64()) {
		return ApplyEmptyTransaction(config, ibs, header, tx, usedGas)
	}
	// TODO: Implement tx.IsTradingTransaction() logic if needed
	// if tx.IsTradingTransaction() && config.IsTIPTomoX(header.Number.Uint64()) {
	// 	return ApplyEmptyTransaction(config, ibs, header, tx, usedGas)
	// }
	// TODO: Implement tx.IsLendingFinalizedTradeTransaction() logic if needed
	// if tx.IsLendingFinalizedTradeTransaction() && config.IsTIPTomoX(header.Number.Uint64()) {
	// 	return ApplyEmptyTransaction(config, ibs, header, tx, usedGas)
	// }

	return nil, nil
}

func ApplySignTransaction(config *chain.Config, ibs *state.IntraBlockState, header *types.Header, tx types.Transaction, usedGas *uint64) (*types.Receipt, error) {
	// Update the state with pending changes
	// In Erigon, we don't explicitly call Finalise/IntermediateRoot here for individual txs usually,
	// but ApplyTransaction does.
	// Here we just skip EVM.

	from, err := tx.Sender(*types.MakeSigner(config, header.Number.Uint64(), header.Time))
	if err != nil {
		return nil, err
	}

	nonce, _ := ibs.GetNonce(from)
	if nonce < tx.GetNonce() {
		return nil, ErrNonceTooHigh
	} else if nonce > tx.GetNonce() {
		return nil, ErrNonceTooLow
	}
	ibs.SetNonce(from, nonce+1)

	// Create a new receipt for the transaction
	// based on the eip phase, we're passing wether the root touch-delete accounts.
	receipt := &types.Receipt{
		Type:              tx.Type(),
		CumulativeGasUsed: *usedGas,
		TxHash:            tx.Hash(),
		GasUsed:           0,
	}

	// Set the receipt logs
	l := &types.Log{
		Address:     config.Viction.ValidatorBlockSignContract,
		Topics:      []common.Hash{},
		Data:        []byte{},
		BlockNumber: header.Number.Uint64(),
	}
	ibs.AddLog(l)

	receipt.Logs = []*types.Log{l}
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	return receipt, nil
}

func ApplyEmptyTransaction(config *chain.Config, ibs *state.IntraBlockState, header *types.Header, tx types.Transaction, usedGas *uint64) (*types.Receipt, error) {
	// Create a new receipt
	receipt := &types.Receipt{
		Type:              tx.Type(),
		CumulativeGasUsed: *usedGas,
		TxHash:            tx.Hash(),
		GasUsed:           0,
	}

	// Set the receipt logs
	l := &types.Log{
		Address:     *tx.GetTo(),
		Topics:      []common.Hash{},
		Data:        []byte{},
		BlockNumber: header.Number.Uint64(),
	}
	ibs.AddLog(l)

	receipt.Logs = []*types.Log{l}
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	return receipt, nil
}

// ApplyVictionBypass applies the address balance bypass logic
func ApplyVictionBypass(header *types.Header, tx types.Transaction, ibs *state.IntraBlockState, config *chain.Config) {
	// Bypass blacklist address
	maxBlockNumber := new(big.Int).SetInt64(9147459)
	if header.Number.Cmp(maxBlockNumber) <= 0 {
		sender, _ := tx.Sender(*types.MakeSigner(config, header.Number.Uint64(), header.Time))
		addrFrom := sender.Hex()
		currentBlockNumber := header.Number.Int64()
		if addr, ok := chain.BypassAddressAtBlock(currentBlockNumber); ok {
			if strings.ToLower(addr) == strings.ToLower(addrFrom) {
				bal, _ := chain.BypassAddressBal(addrFrom)
				hBalance := new(big.Int)
				hBalance.SetString(bal+"000000000000000000", 10)
				log.Info("address", "addr", addr, "with_balance", bal, "TOMO")
				addrBin := common.HexToAddress(addr)
				val, _ := uint256.FromBig(hBalance)
				ibs.SetBalance(addrBin, *val, tracing.BalanceChangeUnspecified)
			}
		}
	}
}
