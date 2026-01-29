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
	// BlockSigners is the address used for block signing transactions
	BlockSigners = common.HexToAddress("0x0000000000000000000000000000000000000088")
	// TradingStateAddr is the address for trading state
	TradingStateAddr = common.HexToAddress("0x0000000000000000000000000000000000000089")
	// TomoXLendingAddress is the address for lending
	TomoXLendingAddress = common.HexToAddress("0x000000000000000000000000000000000000008a")
)

var (
	ErrStopPreparingBlock = errors.New("stop preparing block")
)

// Maps for bypass blacklist logic
var (
	bypassAddrMap  = make(map[string]string)
	bypassBlockMap = make(map[int64]string)
)

func init() {
	bypassAddrMap["0x5248bfb72fd4f234e062d3e9bb76f08643004fcd"] = "29410"
	bypassAddrMap["0x5ac26105b35ea8935be382863a70281ec7a985e9"] = "23551"
	bypassAddrMap["0x09c4f991a41e7ca0645d7dfbfee160b55e562ea4"] = "25821"
	bypassAddrMap["0xb3157bbc5b401a45d6f60b106728bb82ebaa585b"] = "20051"
	bypassAddrMap["0x741277a8952128d5c2ffe0550f5001e4c8247674"] = "23937"
	bypassAddrMap["0x10ba49c1caa97d74b22b3e74493032b180cebe01"] = "27320"
	bypassAddrMap["0x07048d51d9e6179578a6e3b9ee28cdc183b865e4"] = "29758"
	bypassAddrMap["0x4b899001d73c7b4ec404a771d37d9be13b8983de"] = "26148"
	bypassAddrMap["0x85cb320a9007f26b7652c19a2a65db1da2d0016f"] = "27216"
	bypassAddrMap["0x06869dbd0e3a2ea37ddef832e20fa005c6f0ca39"] = "29449"
	bypassAddrMap["0x82e48bc7e2c93d89125428578fb405947764ad7c"] = "28084"
	bypassAddrMap["0x1f9a78534d61732367cbb43fc6c89266af67c989"] = "29287"
	bypassAddrMap["0x7c3b1fa91df55ff7af0cad9e0399384dc5c6641b"] = "21574"
	bypassAddrMap["0x5888dc1ceb0ff632713486b9418e59743af0fd20"] = "28836"
	bypassAddrMap["0xa512fa1c735fc3cc635624d591dd9ea1ce339ca5"] = "25515"
	bypassAddrMap["0x0832517654c7b7e36b1ef45d76de70326b09e2c7"] = "22873"
	bypassAddrMap["0xca14e3c4c78bafb60819a78ff6e6f0f709d2aea7"] = "24968"
	bypassAddrMap["0x652ce195a23035114849f7642b0e06647d13e57a"] = "24091"
	bypassAddrMap["0x29a79f00f16900999d61b6e171e44596af4fb5ae"] = "20790"
	bypassAddrMap["0xf9fd1c2b0af0d91b0b6754e55639e3f8478dd04a"] = "23331"
	bypassAddrMap["0xb835710c9901d5fe940ef1b99ed918902e293e35"] = "28273"
	bypassAddrMap["0x04dd29ce5c253377a9a3796103ea0d9a9e514153"] = "29956"
	bypassAddrMap["0x2b4b56846eaf05c1fd762b5e1ac802efd0ab871c"] = "24911"
	bypassAddrMap["0x1d1f909f6600b23ce05004f5500ab98564717996"] = "25637"
	bypassAddrMap["0x0dfdcebf80006dc9ab7aae8c216b51c6b6759e86"] = "26378"
	bypassAddrMap["0x2b373890a28e5e46197fbc04f303bbfdd344056f"] = "21109"
	bypassAddrMap["0xa8a3ef3dc5d8e36aee76f3671ec501ec31e28254"] = "22072"
	bypassAddrMap["0x4f3d18136fe2b5665c29bdaf74591fc6625ef427"] = "21650"
	bypassAddrMap["0x175d728b0e0f1facb5822a2e0c03bde93596e324"] = "21588"
	bypassAddrMap["0xd575c2611984fcd79513b80ab94f59dc5bab4916"] = "28971"
	bypassAddrMap["0x0579337873c97c4ba051310236ea847f5be41bc0"] = "28344"
	bypassAddrMap["0xed12a519cc15b286920fc15fd86106b3e6a16218"] = "24443"
	bypassAddrMap["0x492d26d852a0a0a2982bb40ec86fe394488c419e"] = "26623"
	bypassAddrMap["0xce5c7635d02dc4e1d6b46c256cae6323be294a32"] = "28459"
	bypassAddrMap["0x8b94db158b5e78a6c032c7e7c9423dec62c8b11c"] = "21803"
	bypassAddrMap["0x0e7c48c085b6b0aa7ca6e4cbcc8b9a92dc270eb4"] = "21739"
	bypassAddrMap["0x206e6508462033ef8425edc6c10789d241d49acb"] = "21883"
	bypassAddrMap["0x7710e7b7682f26cb5a1202e1cff094fbf7777758"] = "28907"
	bypassAddrMap["0xcb06f949313b46bbf53b8e6b2868a0c260ff9385"] = "28932"
	bypassAddrMap["0xf884e43533f61dc2997c0e19a6eff33481920c00"] = "27780"
	bypassAddrMap["0x8b635ef2e4c8fe21fc2bda027eb5f371d6aa2fc1"] = "23115"
	bypassAddrMap["0x10f01a27cf9b29d02ce53497312b96037357a361"] = "22716"
	bypassAddrMap["0x693dd49b0ed70f162d733cf20b6c43dc2a2b4d95"] = "20020"
	bypassAddrMap["0xe0bec72d1c2a7a7fb0532cdfac44ebab9f6f41ee"] = "23071"
	bypassAddrMap["0xc8793633a537938cb49cdbbffd45428f10e45b64"] = "24652"
	bypassAddrMap["0x0d07a6cbbe9fa5c4f154e5623bfe47fb4d857d8e"] = "21907"
	bypassAddrMap["0xd4080b289da95f70a586610c38268d8d4cf1e4c4"] = "22719"
	bypassAddrMap["0x8bcfb0caf41f0aa1b548cae76dcdd02e33866a1b"] = "29062"
	bypassAddrMap["0xabfef22b92366d3074676e77ea911ccaabfb64c1"] = "23110"
	bypassAddrMap["0xcc4df7a32faf3efba32c9688def5ccf9fefe443d"] = "21397"
	bypassAddrMap["0x7ec1e48a582475f5f2b7448a86c4ea7a26ea36f8"] = "23105"
	bypassAddrMap["0xe3de67289080f63b0c2612844256a25bb99ac0ad"] = "29721"
	bypassAddrMap["0x3ba623300cf9e48729039b3c9e0dee9b785d636e"] = "25917"
	bypassAddrMap["0x402f2cfc9c8942f5e7a12c70c625d07a5d52fe29"] = "24712"
	bypassAddrMap["0xd62358d42afbde095a4ca868581d85f9adcc3d61"] = "24449"
	bypassAddrMap["0x3969f86acb733526cd61e3c6e3b4660589f32bc6"] = "29579"
	bypassAddrMap["0x67615413d7cdadb2c435a946aec713a9a9794d39"] = "26333"
	bypassAddrMap["0xfe685f43acc62f92ab01a8da80d76455d39d3cb3"] = "29825"
	bypassAddrMap["0x3538a544021c07869c16b764424c5987409cba48"] = "22746"
	bypassAddrMap["0xe187cf86c2274b1f16e8225a7da9a75aba4f1f5f"] = "23734"

	bypassBlockMap[9073579] = "0x5248bfb72fd4f234e062d3e9bb76f08643004fcd"
	bypassBlockMap[9147130] = "0x5ac26105b35ea8935be382863a70281ec7a985e9"
	bypassBlockMap[9147195] = "0x09c4f991a41e7ca0645d7dfbfee160b55e562ea4"
	bypassBlockMap[9147200] = "0xb3157bbc5b401a45d6f60b106728bb82ebaa585b"
	bypassBlockMap[9147206] = "0x741277a8952128d5c2ffe0550f5001e4c8247674"
	bypassBlockMap[9147212] = "0x10ba49c1caa97d74b22b3e74493032b180cebe01"
	bypassBlockMap[9147217] = "0x07048d51d9e6179578a6e3b9ee28cdc183b865e4"
	bypassBlockMap[9147223] = "0x4b899001d73c7b4ec404a771d37d9be13b8983de"
	bypassBlockMap[9147229] = "0x85cb320a9007f26b7652c19a2a65db1da2d0016f"
	bypassBlockMap[9147234] = "0x06869dbd0e3a2ea37ddef832e20fa005c6f0ca39"
	bypassBlockMap[9147240] = "0x82e48bc7e2c93d89125428578fb405947764ad7c"
	bypassBlockMap[9147246] = "0x1f9a78534d61732367cbb43fc6c89266af67c989"
	bypassBlockMap[9147251] = "0x7c3b1fa91df55ff7af0cad9e0399384dc5c6641b"
	bypassBlockMap[9147257] = "0x5888dc1ceb0ff632713486b9418e59743af0fd20"
	bypassBlockMap[9147263] = "0xa512fa1c735fc3cc635624d591dd9ea1ce339ca5"
	bypassBlockMap[9147268] = "0x0832517654c7b7e36b1ef45d76de70326b09e2c7"
	bypassBlockMap[9147274] = "0xca14e3c4c78bafb60819a78ff6e6f0f709d2aea7"
	bypassBlockMap[9147279] = "0x652ce195a23035114849f7642b0e06647d13e57a"
	bypassBlockMap[9147285] = "0x29a79f00f16900999d61b6e171e44596af4fb5ae"
	bypassBlockMap[9147291] = "0xf9fd1c2b0af0d91b0b6754e55639e3f8478dd04a"
	bypassBlockMap[9147296] = "0xb835710c9901d5fe940ef1b99ed918902e293e35"
	bypassBlockMap[9147302] = "0x04dd29ce5c253377a9a3796103ea0d9a9e514153"
	bypassBlockMap[9147308] = "0x2b4b56846eaf05c1fd762b5e1ac802efd0ab871c"
	bypassBlockMap[9147314] = "0x1d1f909f6600b23ce05004f5500ab98564717996"
	bypassBlockMap[9147319] = "0x0dfdcebf80006dc9ab7aae8c216b51c6b6759e86"
	bypassBlockMap[9147325] = "0x2b373890a28e5e46197fbc04f303bbfdd344056f"
	bypassBlockMap[9147330] = "0xa8a3ef3dc5d8e36aee76f3671ec501ec31e28254"
	bypassBlockMap[9147336] = "0x4f3d18136fe2b5665c29bdaf74591fc6625ef427"
	bypassBlockMap[9147342] = "0x175d728b0e0f1facb5822a2e0c03bde93596e324"
	bypassBlockMap[9145281] = "0xd575c2611984fcd79513b80ab94f59dc5bab4916"
	bypassBlockMap[9145315] = "0x0579337873c97c4ba051310236ea847f5be41bc0"
	bypassBlockMap[9145341] = "0xed12a519cc15b286920fc15fd86106b3e6a16218"
	bypassBlockMap[9145367] = "0x492d26d852a0a0a2982bb40ec86fe394488c419e"
	bypassBlockMap[9145386] = "0xce5c7635d02dc4e1d6b46c256cae6323be294a32"
	bypassBlockMap[9145414] = "0x8b94db158b5e78a6c032c7e7c9423dec62c8b11c"
	bypassBlockMap[9145436] = "0x0e7c48c085b6b0aa7ca6e4cbcc8b9a92dc270eb4"
	bypassBlockMap[9145463] = "0x206e6508462033ef8425edc6c10789d241d49acb"
	bypassBlockMap[9145493] = "0x7710e7b7682f26cb5a1202e1cff094fbf7777758"
	bypassBlockMap[9145519] = "0xcb06f949313b46bbf53b8e6b2868a0c260ff9385"
	bypassBlockMap[9145549] = "0xf884e43533f61dc2997c0e19a6eff33481920c00"
	bypassBlockMap[9147352] = "0x8b635ef2e4c8fe21fc2bda027eb5f371d6aa2fc1"
	bypassBlockMap[9147357] = "0x10f01a27cf9b29d02ce53497312b96037357a361"
	bypassBlockMap[9147363] = "0x693dd49b0ed70f162d733cf20b6c43dc2a2b4d95"
	bypassBlockMap[9147369] = "0xe0bec72d1c2a7a7fb0532cdfac44ebab9f6f41ee"
	bypassBlockMap[9147375] = "0xc8793633a537938cb49cdbbffd45428f10e45b64"
	bypassBlockMap[9147380] = "0x0d07a6cbbe9fa5c4f154e5623bfe47fb4d857d8e"
	bypassBlockMap[9147386] = "0xd4080b289da95f70a586610c38268d8d4cf1e4c4"
	bypassBlockMap[9147392] = "0x8bcfb0caf41f0aa1b548cae76dcdd02e33866a1b"
	bypassBlockMap[9147397] = "0xabfef22b92366d3074676e77ea911ccaabfb64c1"
	bypassBlockMap[9147403] = "0xcc4df7a32faf3efba32c9688def5ccf9fefe443d"
	bypassBlockMap[9147408] = "0x7ec1e48a582475f5f2b7448a86c4ea7a26ea36f8"
	bypassBlockMap[9147414] = "0xe3de67289080f63b0c2612844256a25bb99ac0ad"
	bypassBlockMap[9147420] = "0x3ba623300cf9e48729039b3c9e0dee9b785d636e"
	bypassBlockMap[9147425] = "0x402f2cfc9c8942f5e7a12c70c625d07a5d52fe29"
	bypassBlockMap[9147431] = "0xd62358d42afbde095a4ca868581d85f9adcc3d61"
	bypassBlockMap[9147437] = "0x3969f86acb733526cd61e3c6e3b4660589f32bc6"
	bypassBlockMap[9147442] = "0x67615413d7cdadb2c435a946aec713a9a9794d39"
	bypassBlockMap[9147448] = "0xfe685f43acc62f92ab01a8da80d76455d39d3cb3"
	bypassBlockMap[9147453] = "0x3538a544021c07869c16b764424c5987409cba48"
	bypassBlockMap[9147459] = "0xe187cf86c2274b1f16e8225a7da9a75aba4f1f5f"
}

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
	if to != nil && *to == BlockSigners && config.IsTIPSigning(header.Number.Uint64()) {
		return ApplySignTransaction(config, ibs, header, tx, usedGas)
	}

	if to != nil && *to == TradingStateAddr && config.IsTIPTomoX(header.Number.Uint64()) {
		return ApplyEmptyTransaction(config, ibs, header, tx, usedGas)
	}
	if to != nil && *to == TomoXLendingAddress && config.IsTIPTomoX(header.Number.Uint64()) {
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
		Address:     BlockSigners,
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
		if addr, ok := bypassBlockMap[currentBlockNumber]; ok {
			if strings.ToLower(addr) == strings.ToLower(addrFrom) {
				bal := bypassAddrMap[addr]
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
