// Copyright 2024 The Erigon Authors
// This file is part of Erigon.

package sentry

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/eth63"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/protocols/eth"
)

// ETH63Handler handles ETH/63 protocol messages for compatibility with older Ethereum clients
type ETH63Handler struct {
	logger log.Logger
	sentry *GrpcServer
}

// NewETH63Handler creates a new ETH/63 protocol handler
func NewETH63Handler(sentry *GrpcServer, logger log.Logger) *ETH63Handler {
	return &ETH63Handler{
		logger: logger,
		sentry: sentry,
	}
}

// HandleETH63Message processes incoming ETH/63 messages from Geth 1.9 and similar clients
// Note: This handler is currently unused in production. ETH/63 messages are handled
// directly in sentry_grpc_server.go:runPeer(). This handler may be used for future
// specialized ETH/63 message processing.
func (h *ETH63Handler) HandleETH63Message(ctx context.Context, msgCode uint64, peerID [64]byte, data []byte) error {
	// Context is reserved for future use (cancellation, timeouts)
	_ = ctx
	switch msgCode {
	case eth.TransactionsMsg:
		h.logger.Info("[ETH/63] Handling transactions message", "peer", fmt.Sprintf("%x", peerID[:8]))
		return h.handleETH63Transactions(data, peerID)
	case eth.NewBlockHashesMsg:
		return h.handleETH63NewBlockHashes(data, peerID)
	case eth.NewBlockMsg:
		return h.handleETH63NewBlock(data, peerID)
	case eth.GetBlockHeadersMsg:
		return h.handleETH63GetBlockHeaders(data, peerID)
	case eth.BlockHeadersMsg:
		return h.handleETH63BlockHeaders(data, peerID)
	case eth.GetBlockBodiesMsg:
		return h.handleETH63GetBlockBodies(data, peerID)
	case eth.BlockBodiesMsg:
		return h.handleETH63BlockBodies(data, peerID)
	case eth.GetReceiptsMsg:
		return h.handleETH63GetReceipts(data, peerID)
	case eth.ReceiptsMsg:
		return h.handleETH63Receipts(data, peerID)
	default:
		h.logger.Info("[ETH/63] Unknown message code", "code", msgCode, "peer", fmt.Sprintf("%x", peerID[:8]))
		return nil
	}
}

// handleETH63Transactions processes transaction messages from ETH/63 clients
func (h *ETH63Handler) handleETH63Transactions(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received transactions from Geth 1.9 client",
		"peer", fmt.Sprintf("%x", peerID[:8]),
		"data_size", len(data))

	// Decode ETH/63 transactions using the ETH63Transaction type
	var transactions eth63.ETH63Transactions
	if err := rlp.DecodeBytes(data, &transactions); err != nil {
		h.logger.Error("[ETH/63] Failed to decode transactions", "err", err)
		return err
	}

	h.logger.Info("[ETH/63] Decoded transactions",
		"count", len(transactions),
		"peer", fmt.Sprintf("%x", peerID[:8]))

	// Process each transaction
	for i, tx := range transactions {
		h.logger.Info("[ETH/63] Processing transaction",
			"index", i+1,
			"hash", tx.Hash().Hex(),
			"value", tx.Value().String(),
			"gas", tx.Gas(),
			"gasPrice", tx.GasPrice().String(),
			"nonce", tx.Nonce())

		// Validate transaction signature
		if err := h.validateETH63Transaction(tx); err != nil {
			h.logger.Warn("[ETH/63] Invalid transaction signature",
				"hash", tx.Hash().Hex(), "err", err)
			continue
		}

		// Forward to Erigon's transaction pool via gRPC
		if err := h.forwardTransactionToErigon(tx); err != nil {
			h.logger.Error("[ETH/63] Failed to forward transaction to Erigon",
				"hash", tx.Hash().Hex(), "err", err)
		}
	}

	return nil
}

// validateETH63Transaction validates the transaction signature using ETH/63 signing
func (h *ETH63Handler) validateETH63Transaction(tx *eth63.ETH63Transaction) error {
	// Check if transaction has signature
	v, r, s := tx.RawSignatureValues()
	if v.Sign() == 0 && r.Sign() == 0 && s.Sign() == 0 {
		return fmt.Errorf("transaction has no signature")
	}

	// Determine the signer based on transaction type
	var signer eth63.Signer
	if tx.Protected() {
		// EIP-155 transaction
		chainId := tx.ChainId()
		signer = eth63.NewEIP155Signer(chainId)
	} else {
		// Legacy transaction (pre-EIP-155)
		signer = eth63.HomesteadSigner{}
	}

	// Verify signature and get sender
	sender, err := eth63.Sender(signer, tx)
	if err != nil {
		return fmt.Errorf("failed to recover sender: %w", err)
	}

	h.logger.Info("[ETH/63] Transaction signature validated",
		"hash", tx.Hash().Hex(),
		"sender", sender.Hex(),
		"protected", tx.Protected())

	return nil
}

// handleETH63NewBlockHashes processes new block hash announcements from ETH/63 clients
func (h *ETH63Handler) handleETH63NewBlockHashes(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received new block hashes",
		"peer", fmt.Sprintf("%x", peerID[:8]),
		"data_size", len(data))

	// Decode ETH/63 block hash announcements
	var announcements []struct {
		Hash   common.Hash
		Number uint64
	}
	if err := rlp.DecodeBytes(data, &announcements); err != nil {
		h.logger.Error("[ETH/63] Failed to decode block hashes", "err", err)
		return err
	}

	for _, announcement := range announcements {
		h.logger.Info("[ETH/63] New block announced",
			"hash", announcement.Hash.Hex(),
			"number", announcement.Number,
			"peer", fmt.Sprintf("%x", peerID[:8]))
	}

	return nil
}

// handleETH63NewBlock processes new block messages from ETH/63 clients
func (h *ETH63Handler) handleETH63NewBlock(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received new block",
		"peer", fmt.Sprintf("%x", peerID[:8]),
		"data_size", len(data))

	// Forward to Erigon for processing
	return h.forwardToErigon(proto_sentry.MessageId_NEW_BLOCK_63, peerID, data)
}

// handleETH63GetBlockHeaders processes block header requests from ETH/63 clients
func (h *ETH63Handler) handleETH63GetBlockHeaders(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received get block headers request",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	// Forward to Erigon for processing
	return h.forwardToErigon(proto_sentry.MessageId_GET_BLOCK_HEADERS_63, peerID, data)
}

// handleETH63BlockHeaders processes block header responses from ETH/63 clients
func (h *ETH63Handler) handleETH63BlockHeaders(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received block headers",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	return h.forwardToErigon(proto_sentry.MessageId_BLOCK_HEADERS_63, peerID, data)
}

// handleETH63GetBlockBodies processes block body requests from ETH/63 clients
func (h *ETH63Handler) handleETH63GetBlockBodies(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received get block bodies request",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	return h.forwardToErigon(proto_sentry.MessageId_GET_BLOCK_BODIES_63, peerID, data)
}

// handleETH63BlockBodies processes block body responses from ETH/63 clients
func (h *ETH63Handler) handleETH63BlockBodies(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received block bodies",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	return h.forwardToErigon(proto_sentry.MessageId_BLOCK_BODIES_63, peerID, data)
}

// handleETH63GetReceipts processes receipt requests from ETH/63 clients
func (h *ETH63Handler) handleETH63GetReceipts(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received get receipts request",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	return h.forwardToErigon(proto_sentry.MessageId_GET_RECEIPTS_63, peerID, data)
}

// handleETH63Receipts processes receipt responses from ETH/63 clients
func (h *ETH63Handler) handleETH63Receipts(data []byte, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Received receipts",
		"peer", fmt.Sprintf("%x", peerID[:8]))

	return h.forwardToErigon(proto_sentry.MessageId_RECEIPTS_63, peerID, data)
}

// forwardTransactionToErigon forwards a transaction to Erigon's transaction pool
func (h *ETH63Handler) forwardTransactionToErigon(tx *eth63.ETH63Transaction) error {
	// Create a transactions message compatible with Erigon
	transactions := eth63.ETH63Transactions{tx}
	payload, err := rlp.EncodeToBytes(transactions)
	if err != nil {
		return fmt.Errorf("failed to encode transactions payload: %w", err)
	}

	h.logger.Info("[ETH/63] Forwarding transaction to Erigon",
		"hash", tx.Hash().Hex(),
		"payload_size", len(payload))

	// Forward to Erigon via the sentry's message routing
	h.sentry.send(proto_sentry.MessageId_TRANSACTIONS_63, [64]byte{}, payload)

	// Log the transaction details
	h.logger.Info("[ETH/63] Transaction forwarded to Erigon",
		"hash", tx.Hash().Hex(),
		"nonce", tx.Nonce(),
		"value", tx.Value().String(),
		"gas_price", tx.GasPrice().String(),
		"gas_limit", tx.Gas())
	return nil
}

// forwardToErigon forwards a message to Erigon's internal handlers
func (h *ETH63Handler) forwardToErigon(msgID proto_sentry.MessageId, peerID [64]byte, data []byte) error {
	h.logger.Info("[ETH/63] �� Forwarding message to Erigon",
		"message", msgID.String(),
		"peer", fmt.Sprintf("%x", peerID[:8]),
		"data_size", len(data))

	h.sentry.send(msgID, peerID, data)
	return nil
}

// GenerateETH63Transaction creates a sample transaction for testing ETH/63 compatibility
func (h *ETH63Handler) GenerateETH63Transaction() *eth63.ETH63Transaction {
	h.logger.Info("[ETH/63] Generating sample transaction for testing")

	// Generate random transaction data for testing
	nonce := uint64(time.Now().Unix())
	gasLimit := uint64(21000)
	gasPrice := big.NewInt(20000000000)      // 20 Gwei
	value := big.NewInt(1000000000000000000) // 1 ETH

	// Random recipient address
	to := common.Address{}
	rand.Read(to[:])

	// Create ETH63Transaction (compatible with old go-ethereum interface)
	tx := eth63.NewETH63Transaction(nonce, to, value, gasLimit, gasPrice, nil)

	h.logger.Info("[ETH/63] Generated transaction",
		"hash", tx.Hash().Hex(),
		"nonce", nonce,
		"to", to.Hex(),
		"value", value.String(),
		"gasPrice", gasPrice.String())

	return tx
}

// SendETH63TransactionToPeer sends a transaction to an ETH/63 peer
func (h *ETH63Handler) SendETH63TransactionToPeer(tx *eth63.ETH63Transaction, peerID [64]byte) error {
	h.logger.Info("[ETH/63] Sending transaction to ETH/63 peer",
		"hash", tx.Hash().Hex(),
		"peer", fmt.Sprintf("%x", peerID[:8]))

	// Encode transaction for ETH/63 format (no request ID)
	transactions := eth63.ETH63Transactions{tx}
	payload, err := rlp.EncodeToBytes(transactions)
	if err != nil {
		return fmt.Errorf("failed to encode ETH/63 transaction: %w", err)
	}

	// Find the peer and send the message
	peerInfo := h.sentry.getPeer(peerID)
	if peerInfo == nil {
		return fmt.Errorf("peer not found: %x", peerID[:8])
	}

	// Send via P2P protocol (ETH/63 transaction message)
	h.sentry.writePeer("[ETH/63] sendTransaction", peerInfo, eth.TransactionsMsg, payload, 0)

	h.logger.Info("[ETH/63] Transaction sent to peer",
		"hash", tx.Hash().Hex(),
		"peer", fmt.Sprintf("%x", peerID[:8]),
		"payload_size", len(payload))

	return nil
}

// BroadcastETH63Transaction broadcasts a transaction to all ETH/63 peers
func (h *ETH63Handler) BroadcastETH63Transaction(tx *eth63.ETH63Transaction) error {
	h.logger.Info("[ETH/63] Broadcasting transaction to all ETH/63 peers",
		"hash", tx.Hash().Hex())

	count := 0
	h.sentry.rangePeers(func(peerInfo *PeerInfo) bool {
		// Check if peer is using ETH/63 protocol
		if peerInfo.protocol == 63 {
			peerID := peerInfo.ID()
			if err := h.SendETH63TransactionToPeer(tx, peerID); err != nil {
				h.logger.Error("[ETH/63] Failed to send transaction to peer",
					"peer", fmt.Sprintf("%x", peerID[:8]), "err", err)
			} else {
				count++
			}
		}
		return true
	})

	h.logger.Info("[ETH/63] Transaction broadcasted",
		"hash", tx.Hash().Hex(),
		"peers_count", count)

	return nil
}

// DecodeFromETH63 decodes ETH/63 transaction data (for backward compatibility)
func DecodeFromETH63(data []byte) (eth63.ETH63Transactions, error) {
	var transactions eth63.ETH63Transactions
	if err := rlp.DecodeBytes(data, &transactions); err != nil {
		return nil, err
	}
	return transactions, nil
}
