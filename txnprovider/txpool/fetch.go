// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/eth63"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
)

// Fetch connects to sentry and implements eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from txn pool
type Fetch struct {
	ctx                      context.Context // Context used for cancellation and closing of the fetcher
	pool                     Pool            // Transaction pool implementation
	db                       kv.RwDB
	stateChangesClient       StateChangesClient
	wg                       *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	stateChangesParseCtx     *TxnParseContext
	pooledTxnsParseCtx       *TxnParseContext
	sentryClients            []sentry.SentryClient // sentry clients that will be used for accessing the network
	stateChangesParseCtxLock sync.Mutex
	pooledTxnsParseCtxLock   sync.Mutex
	logger                   log.Logger
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(
	ctx context.Context,
	sentryClients []sentry.SentryClient,
	pool Pool,
	stateChangesClient StateChangesClient,
	db kv.RwDB,
	chainID uint256.Int,
	logger log.Logger,
	opts ...Option,
) *Fetch {
	options := applyOpts(opts...)
	f := &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: NewTxnParseContext(chainID).ChainIDRequired(), //TODO: change ctx if rules changed
		pooledTxnsParseCtx:   NewTxnParseContext(chainID).ChainIDRequired(),
		wg:                   options.p2pFetcherWg,
		logger:               logger,
	}
	f.pooledTxnsParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)
	f.stateChangesParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)

	return f
}

func (f *Fetch) threadSafeParsePooledTxn(cb func(*TxnParseContext) error) error {
	f.pooledTxnsParseCtxLock.Lock()
	defer f.pooledTxnsParseCtxLock.Unlock()
	return cb(f.pooledTxnsParseCtx)
}

func (f *Fetch) threadSafeParseStateChangeTxn(cb func(*TxnParseContext) error) error {
	f.stateChangesParseCtxLock.Lock()
	defer f.stateChangesParseCtxLock.Unlock()
	return cb(f.stateChangesParseCtx)
}

// ConnectSentries initialises connection to the sentry
func (f *Fetch) ConnectSentries() {
	for i := range f.sentryClients {
		go func(i int) {
			f.receiveMessageLoop(f.sentryClients[i])
		}(i)
		go func(i int) {
			f.receivePeerLoop(f.sentryClients[i])
		}(i)
	}
}

func (f *Fetch) ConnectCore() {
	go func() {
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if err := f.handleStateChanges(f.ctx, f.stateChangesClient); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				f.logger.Warn("[txpool.handleStateChanges]", "err", err)
			}
		}
	}()
}

func (f *Fetch) receiveMessageLoop(sentryClient sentry.SentryClient) {
	fmt.Println("------> receiveMessageLoop: Starting receiveMessageLoop")
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvMessage] sentry not ready yet", "err", err)
			continue
		}
		if err := f.receiveMessage(f.ctx, sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			f.logger.Warn("[txpool.recvMessage]", "err", err)
		}
	}
}

func (f *Fetch) receiveMessage(ctx context.Context, sentryClient sentry.SentryClient) error {
	fmt.Println("------> receiveMessage: Starting receiveMessage")
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := sentryClient.Messages(streamCtx, &sentry.MessagesRequest{Ids: []sentry.MessageId{
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66,
		sentry.MessageId_TRANSACTIONS_66,
		sentry.MessageId_POOLED_TRANSACTIONS_66,
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		sentry.MessageId_TRANSACTIONS_63,
	}}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-f.ctx.Done():
			return ctx.Err()
		default:
		}
		return err
	}
	var req *sentry.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-f.ctx.Done():
				return ctx.Err()
			default:
			}
			return fmt.Errorf("txpool.receiveMessage: %w", err)
		}
		if req == nil {
			return nil
		}
		if err = f.handleInboundMessage(streamCtx, req, sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			fmt.Println("[txpool.fetch] Handling incoming message", "reqID", req.Id.String(), "err", err)
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient) (err error) {
	fmt.Println("------> handleInboundMessage: Starting handleInboundMessage::1", req.Id.String())
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s, rlp: %x", rec, dbg.Stack(), req.Data)
		}
	}()

	if !f.pool.Started() {
		return nil
	}
	tx, err := f.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Convert peer ID for logging
	peerID := gointerfaces.ConvertH512ToHash(req.PeerId)
	peerHex := hex.EncodeToString(peerID[:])
	fmt.Println("------> handleInboundMessage: Starting handleInboundMessage", req.Id.String())
	switch req.Id {
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		// STEP 1: Node 2 receives transaction hashes from Node 1
		f.logger.Info("[NODE2] 📨 STEP 1: Received transaction hashes from peer",
			"peer_id", peerHex,
			"data_size", len(req.Data),
			"message_type", "NEW_POOLED_TRANSACTION_HASHES_66")

		hashCount, pos, err := ParseHashesCount(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
		}

		f.logger.Info("[NODE2] 🏷️ Parsed transaction hashes",
			"hash_count", hashCount)

		hashes := make([]byte, 32*hashCount)
		for i := 0; i < len(hashes); i += 32 {
			if _, pos, err = ParseHash(req.Data, pos, hashes[i:]); err != nil {
				return err
			}
		}

		// Log each hash received
		for i := 0; i < hashCount; i++ {
			hash := hashes[i*32 : (i+1)*32]
			f.logger.Info("[NODE2] 🔍 Received transaction hash",
				"hash_index", i+1,
				"hash", hex.EncodeToString(hash))
		}

		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}

		f.logger.Info("[NODE2] 🔍 Filtered unknown hashes",
			"total_hashes", hashCount,
			"unknown_hashes", len(unknownHashes)/32)

		if len(unknownHashes) > 0 {
			// STEP 2: Node 2 requests full transactions from Node 1
			f.logger.Info("[NODE2]  STEP 2: Requesting full transactions from peer",
				"peer_id", peerHex,
				"unknown_count", len(unknownHashes)/32)

			var encodedRequest []byte
			var messageID sentry.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentry.MessageId_GET_POOLED_TRANSACTIONS_66

			// Log the request details
			f.logger.Info("[NODE2] Sending GET_POOLED_TRANSACTIONS_66 request",
				"peer_id", peerHex,
				"request_size", len(encodedRequest),
				"request_id", uint64(1))

			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				f.logger.Error("[NODE2] ❌ Failed to send GET_POOLED_TRANSACTIONS_66 request",
					"peer_id", peerHex,
					"error", err)
				return err
			}

			f.logger.Info("[NODE2] ✅ Successfully sent GET_POOLED_TRANSACTIONS_66 request",
				"peer_id", peerHex)
		} else {
			f.logger.Info("[NODE2] ℹ️ All transaction hashes already known, no request needed")
		}

	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
		// Handle eth/68 transaction hash announcements
		f.logger.Info("[NODE2] 📨 Received NEW_POOLED_TRANSACTION_HASHES_68 from peer",
			"peer_id", peerHex,
			"data_size", len(req.Data))

		_, _, hashes, _, err := rlp.ParseAnnouncements(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes88: %w", err)
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}

		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentry.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentry.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}

	case sentry.MessageId_GET_POOLED_TRANSACTIONS_66:
		// STEP 3: Node 2 receives a request for transactions (when acting as Node 1)
		f.logger.Info("[NODE2] 📥 STEP 3: Received GET_POOLED_TRANSACTIONS_66 request from peer",
			"peer_id", peerHex,
			"data_size", len(req.Data))

		var encodedRequest []byte
		var messageID sentry.MessageId
		messageID = sentry.MessageId_POOLED_TRANSACTIONS_66
		requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
		if err != nil {
			return err
		}

		f.logger.Info("[NODE2] 🔍 Processing GET_POOLED_TRANSACTIONS_66 request",
			"peer_id", peerHex,
			"request_id", requestID,
			"hash_count", len(hashes)/32)

		// limit to max 256 transactions in a reply
		const hashSize = 32
		hashes = hashes[:min(len(hashes), 256*hashSize)]

		var txns [][]byte
		responseSize := 0
		processed := len(hashes)

		for i := 0; i < len(hashes); i += hashSize {
			if responseSize >= p2pTxPacketLimit {
				processed = i
				log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply truncated to fit p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
				break
			}

			txnHash := hashes[i:min(i+hashSize, len(hashes))]
			txn, err := f.pool.GetRlp(tx, txnHash)
			if err != nil {
				return err
			}
			if txn == nil {
				f.logger.Warn("[NODE2] ⚠️ Transaction not found in pool",
					"hash", hex.EncodeToString(txnHash))
				continue
			}

			f.logger.Info("[NODE2] ✅ Found transaction in pool",
				"hash", hex.EncodeToString(txnHash),
				"txn_size", len(txn))

			txns = append(txns, txn)
			responseSize += len(txn)
		}

		f.logger.Info("[NODE2] 📤 Preparing POOLED_TRANSACTIONS_66 response",
			"peer_id", peerHex,
			"request_id", requestID,
			"found_txns", len(txns),
			"response_size", responseSize)

		encodedRequest = EncodePooledTransactions66(txns, requestID, nil)
		if len(encodedRequest) > p2pTxPacketLimit {
			log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply exceeds p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
		}

		if _, err := sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			f.logger.Error("[NODE2] ❌ Failed to send POOLED_TRANSACTIONS_66 response",
				"peer_id", peerHex,
				"error", err)
			return err
		}

		f.logger.Info("[NODE2] ✅ Successfully sent POOLED_TRANSACTIONS_66 response",
			"peer_id", peerHex,
			"response_size", len(encodedRequest))

	case sentry.MessageId_POOLED_TRANSACTIONS_66, sentry.MessageId_TRANSACTIONS_66:
		// STEP 4: Node 2 receives full transactions from Node 1
		messageType := "TRANSACTIONS_66"
		if req.Id == sentry.MessageId_POOLED_TRANSACTIONS_66 {
			messageType = "POOLED_TRANSACTIONS_66"
		}

		f.logger.Info("[NODE2] 💰 STEP 4: Received full transactions from peer",
			"peer_id", peerHex,
			"message_type", messageType,
			"data_size", len(req.Data))

		txns := TxnSlots{}
		if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
			return nil
		}); err != nil {
			return err
		}

		switch req.Id {
		case sentry.MessageId_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, err := ParseTransactions(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		case sentry.MessageId_POOLED_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, _, err := ParsePooledTransactions66(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}

		f.logger.Info("[NODE2] 📊 Parsed transactions",
			"transaction_count", len(txns.Txns))

		if len(txns.Txns) == 0 {
			f.logger.Info("[NODE2] ℹ️ No new transactions to add to pool")
			return nil
		}

		f.pool.AddRemoteTxns(ctx, txns)
		f.logger.Info("[NODE2] ✅ STEP 4 COMPLETE: Added transactions to pool",
			"added_count", len(txns.Txns),
			"peer_id", peerHex)

		// Around line 489-538, replace the entire case block:
	case sentry.MessageId_TRANSACTIONS_63:
		fmt.Println("------> handleInboundMessage: Received ETH63 transaction")
		f.logger.Info("[NODE2] 💰 Received ETH/63 transactions from peer",
			"peer_id", peerHex,
			"data_size", len(req.Data))

		// Decode ETH/63 transactions as ETH63Transactions (list of transactions)
		var eth63Txs eth63.ETH63Transactions
		if err := rlp.DecodeBytes(req.Data, &eth63Txs); err != nil {
			fmt.Printf("❌ Failed to decode ETH/63 transactions: %v\n", err)
			f.logger.Error("[txpool.fetch] Failed to decode ETH/63 transactions",
				"err", err,
				"peer_id", peerHex,
				"data_size", len(req.Data),
				"data_preview", fmt.Sprintf("%x", req.Data[:min(100, len(req.Data))]))
			return fmt.Errorf("failed to decode ETH/63 transactions: %w", err)
		}

		f.logger.Info("[txpool.fetch] Decoded ETH/63 transactions",
			"count", len(eth63Txs),
			"peer_id", peerHex)

		if len(eth63Txs) == 0 {
			f.logger.Info("[txpool.fetch] No transactions in ETH/63 message")
			return nil
		}

		// Convert ETH/63 transactions to TxnSlots and add to pool
		txns := TxnSlots{}
		if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
			// Resize to accommodate all transactions
			txns.Resize(uint(len(eth63Txs)))

			for i, eth63Tx := range eth63Txs {
				// Encode ETH/63 transaction to RLP bytes
				txRlp, err := rlp.EncodeToBytes(eth63Tx)
				if err != nil {
					f.logger.Error("[txpool.fetch] Failed to encode ETH/63 transaction to RLP",
						"index", i, "err", err)
					// Skip this transaction
					txns.Resize(uint(i))
					break
				}

				// Initialize TxnSlot
				txns.Txns[i] = &TxnSlot{}

				// Parse the RLP into TxnSlot
				// ETH/63 transactions are legacy format (no envelope, no blobs)
				_, err = parseContext.ParseTransaction(txRlp, 0, txns.Txns[i], txns.Senders.At(i),
					false /* hasEnvelope */, false, /* wrappedWithBlobs */
					func(hash []byte) error {
						// Check if transaction is already known
						known, err := f.pool.IdHashKnown(tx, hash)
						if err != nil {
							return err
						}
						if known {
							return ErrRejected
						}
						return nil
					})
				if err != nil {
					if errors.Is(err, ErrRejected) {
						// Transaction already known, skip it
						f.logger.Debug("[txpool.fetch] ETH/63 transaction already known",
							"index", i, "hash", fmt.Sprintf("%x", eth63Tx.Hash()))
						txns.Resize(uint(i))
						break
					}
					f.logger.Error("[txpool.fetch] Failed to parse ETH/63 transaction",
						"index", i, "hash", fmt.Sprintf("%x", eth63Tx.Hash()), "err", err)
					// Remove this transaction from the list
					txns.Resize(uint(i))
					break
				}

				// Mark as remote transaction
				txns.IsLocal[i] = false

				// Log details for first few transactions
				if i < 5 {
					f.logger.Debug("[txpool.fetch] Parsed ETH/63 transaction",
						"index", i,
						"hash", fmt.Sprintf("%x", eth63Tx.Hash()),
						"nonce", eth63Tx.Nonce(),
						"gas", eth63Tx.Gas(),
						"gasPrice", eth63Tx.GasPrice().String(),
						"value", eth63Tx.Value().String())
				}
			}

			return nil
		}); err != nil {
			return err
		}

		// Resize to actual number of successfully parsed transactions
		actualCount := len(txns.Txns)
		for i := 0; i < actualCount; i++ {
			if txns.Txns[i] == nil {
				actualCount = i
				break
			}
		}
		txns.Resize(uint(actualCount))

		if len(txns.Txns) == 0 {
			f.logger.Info("[txpool.fetch] No valid ETH/63 transactions to add")
			return nil
		}

		// Add transactions to pool
		f.pool.AddRemoteTxns(ctx, txns)
		f.logger.Info("[txpool.fetch] ✅ Added ETH/63 transactions to pool",
			"added_count", len(txns.Txns),
			"peer_id", peerHex)
		return nil

	default:
		defer f.logger.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

func (f *Fetch) receivePeerLoop(sentryClient sentry.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvPeers] sentry not ready yet", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if err := f.receivePeer(sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}

			f.logger.Warn("[txpool.recvPeers]", "err", err)
		}
	}
}

func (f *Fetch) receivePeer(sentryClient sentry.SentryClient) error {
	streamCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stream, err := sentryClient.PeerEvents(streamCtx, &sentry.PeerEventsRequest{})
	if err != nil {
		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
		}
		return err
	}

	var req *sentry.PeerEvent
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err = f.handleNewPeer(req); err != nil {
			return err
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleNewPeer(req *sentry.PeerEvent) error {
	if req == nil {
		return nil
	}
	switch req.EventId {
	case sentry.PeerEvent_Connect:
		f.pool.AddNewGoodPeer(req.PeerId)
	}

	return nil
}

func (f *Fetch) handleStateChanges(ctx context.Context, client StateChangesClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remote.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err := f.handleStateChangesRequest(ctx, req); err != nil {
			f.logger.Warn("[fetch] onNewBlock", "err", err)
		}

		if f.wg != nil { // to help tests
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleStateChangesRequest(ctx context.Context, req *remote.StateChangeBatch) error {
	var unwindTxns, unwindBlobTxns, minedTxns TxnSlots
	for _, change := range req.ChangeBatch {
		if change.Direction == remote.Direction_FORWARD {
			minedTxns.Resize(uint(len(change.Txs)))
			for i := range change.Txs {
				minedTxns.Txns[i] = &TxnSlot{}
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, minedTxns.Txns[i], minedTxns.Senders.At(i), false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					return err
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		} else if change.Direction == remote.Direction_UNWIND {
			for i := range change.Txs {
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					utx := &TxnSlot{}
					sender := make([]byte, 20)
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, utx, sender, false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					if err != nil {
						return err
					}
					if utx.Type == BlobTxnType {
						unwindBlobTxns.Append(utx, sender, false)
					} else {
						unwindTxns.Append(utx, sender, false)
					}
					return nil
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		}
	}

	if err := f.pool.OnNewBlock(ctx, req, unwindTxns, unwindBlobTxns, minedTxns); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
