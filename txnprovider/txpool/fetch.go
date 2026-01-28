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
	"bytes"
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
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
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
			f.logger.Warn("[txpool.fetch] Error handling incoming message",
				"req_id", req.Id.String(), "err", err)
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient) (err error) {
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
	fmt.Println("------> handleInboundMessage: peerHex", peerHex)

	switch req.Id {
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		return f.handleNewPooledTransactionHashes66(ctx, req, sentryClient, tx, peerHex)

	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
		return f.handleNewPooledTransactionHashes68(ctx, req, sentryClient, tx, peerHex)

	case sentry.MessageId_GET_POOLED_TRANSACTIONS_66:
		return f.handleGetPooledTransactions66(ctx, req, sentryClient, tx, peerHex)

	case sentry.MessageId_POOLED_TRANSACTIONS_66, sentry.MessageId_TRANSACTIONS_66:
		return f.handlePooledTransactions66(ctx, req, tx, peerHex)

	case sentry.MessageId_TRANSACTIONS_63:
		fmt.Println("------> handleInboundMessage: TRANSACTIONS_63")
		return f.handleTransactions63(ctx, req, tx, peerHex)

	default:
		defer f.logger.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

// handleNewPooledTransactionHashes66 handles NEW_POOLED_TRANSACTION_HASHES_66 messages
func (f *Fetch) handleNewPooledTransactionHashes66(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient, tx kv.Tx, peerHex string) error {
	f.logger.Info("[txpool.fetch] Received NEW_POOLED_TRANSACTION_HASHES_66",
		"peer_id", peerHex,
		"data_size", len(req.Data))

	hashCount, pos, err := ParseHashesCount(req.Data, 0)
	if err != nil {
		return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
	}

	hashes := make([]byte, 32*hashCount)
	for i := 0; i < len(hashes); i += 32 {
		if _, pos, err = ParseHash(req.Data, pos, hashes[i:]); err != nil {
			return err
		}
	}

	unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
	if err != nil {
		return err
	}

	if len(unknownHashes) > 0 {
		encodedRequest, err := EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil)
		if err != nil {
			return err
		}

		if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: sentry.MessageId_GET_POOLED_TRANSACTIONS_66, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			f.logger.Error("[txpool.fetch] Failed to send GET_POOLED_TRANSACTIONS_66 request",
				"peer_id", peerHex, "error", err)
			return err
		}

		f.logger.Info("[txpool.fetch] Requested transactions",
			"peer_id", peerHex,
			"unknown_count", len(unknownHashes)/32)
	}

	return nil
}

// handleNewPooledTransactionHashes68 handles NEW_POOLED_TRANSACTION_HASHES_68 messages
func (f *Fetch) handleNewPooledTransactionHashes68(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient, tx kv.Tx, peerHex string) error {
	f.logger.Info("[txpool.fetch] Received NEW_POOLED_TRANSACTION_HASHES_68",
		"peer_id", peerHex,
		"data_size", len(req.Data))

	_, _, hashes, _, err := rlp.ParseAnnouncements(req.Data, 0)
	if err != nil {
		return fmt.Errorf("parsing NewPooledTransactionHashes68: %w", err)
	}

	unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
	if err != nil {
		return err
	}

	if len(unknownHashes) > 0 {
		encodedRequest, err := EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil)
		if err != nil {
			return err
		}

		if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: sentry.MessageId_GET_POOLED_TRANSACTIONS_66, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	}

	return nil
}

// handleGetPooledTransactions66 handles GET_POOLED_TRANSACTIONS_66 requests
func (f *Fetch) handleGetPooledTransactions66(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient, tx kv.Tx, peerHex string) error {
	f.logger.Info("[txpool.fetch] Received GET_POOLED_TRANSACTIONS_66 request",
		"peer_id", peerHex,
		"data_size", len(req.Data))

	requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
	if err != nil {
		return err
	}

	// Limit to max 256 transactions in a reply
	const hashSize = 32
	hashes = hashes[:min(len(hashes), 256*hashSize)]

	var txns [][]byte
	responseSize := 0
	processed := len(hashes)

	for i := 0; i < len(hashes); i += hashSize {
		if responseSize >= p2pTxPacketLimit {
			processed = i
			f.logger.Trace("[txpool.fetch] PooledTransactions reply truncated",
				"requested", len(hashes), "processed", processed)
			break
		}

		txnHash := hashes[i:min(i+hashSize, len(hashes))]
		txn, err := f.pool.GetRlp(tx, txnHash)
		if err != nil {
			return err
		}
		if txn == nil {
			continue
		}

		txns = append(txns, txn)
		responseSize += len(txn)
	}

	encodedResponse := EncodePooledTransactions66(txns, requestID, nil)
	if len(encodedResponse) > p2pTxPacketLimit {
		f.logger.Trace("[txpool.fetch] PooledTransactions reply exceeds limit",
			"requested", len(hashes), "processed", processed)
	}

	if _, err := sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
		Data:   &sentry.OutboundMessageData{Id: sentry.MessageId_POOLED_TRANSACTIONS_66, Data: encodedResponse},
		PeerId: req.PeerId,
	}, &grpc.EmptyCallOption{}); err != nil {
		f.logger.Error("[txpool.fetch] Failed to send POOLED_TRANSACTIONS_66 response",
			"peer_id", peerHex, "error", err)
		return err
	}

	f.logger.Info("[txpool.fetch] Sent POOLED_TRANSACTIONS_66 response",
		"peer_id", peerHex,
		"request_id", requestID,
		"txn_count", len(txns),
		"response_size", len(encodedResponse))

	return nil
}

// handlePooledTransactions66 handles POOLED_TRANSACTIONS_66 and TRANSACTIONS_66 messages
func (f *Fetch) handlePooledTransactions66(ctx context.Context, req *sentry.InboundMessage, tx kv.Tx, peerHex string) error {
	messageType := "TRANSACTIONS_66"
	if req.Id == sentry.MessageId_POOLED_TRANSACTIONS_66 {
		messageType = "POOLED_TRANSACTIONS_66"
	}

	f.logger.Info("[txpool.fetch] Received transactions",
		"peer_id", peerHex,
		"message_type", messageType,
		"data_size", len(req.Data))

	txns := TxnSlots{}
	validateHash := func(hash []byte) error {
		known, err := f.pool.IdHashKnown(tx, hash)
		if err != nil {
			return err
		}
		if known {
			return ErrRejected
		}
		return nil
	}

	if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
		switch req.Id {
		case sentry.MessageId_TRANSACTIONS_66:
			_, err := ParseTransactions(req.Data, 0, parseContext, &txns, validateHash)
			return err
		case sentry.MessageId_POOLED_TRANSACTIONS_66:
			_, _, err := ParsePooledTransactions66(req.Data, 0, parseContext, &txns, validateHash)
			return err
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}
	}); err != nil {
		return err
	}

	if len(txns.Txns) == 0 {
		f.logger.Info("[txpool.fetch] No new transactions to add")
		return nil
	}

	f.pool.AddRemoteTxns(ctx, txns)
	f.logger.Info("[txpool.fetch] Added transactions to pool",
		"peer_id", peerHex,
		"added_count", len(txns.Txns))

	return nil
}

// handleTransactions63 handles TRANSACTIONS_63 (ETH/63) messages
func (f *Fetch) handleTransactions63(ctx context.Context, req *sentry.InboundMessage, tx kv.Tx, peerHex string) error {
	f.logger.Info("[txpool.fetch] Received ETH/63 transactions",
		"peer_id", peerHex,
		"data_size", len(req.Data))

	// ETH/63 TransactionsMsg payload is an RLP list of transactions.
	// For PoSV ETH/63, these are expected to be legacy transactions (RLP list per txn, no typed envelope).
	s := rlp.NewStream(bytes.NewReader(req.Data), uint64(len(req.Data)))
	_, err := s.List()
	if err != nil {
		f.logger.Error("[txpool.fetch] Failed to open ETH/63 transactions list", "err", err, "peer_id", peerHex)
		return fmt.Errorf("decode ETH/63 tx list: %w", err)
	}

	rawTxs := make([][]byte, 0, 16)
	for {
		raw, err := s.Raw()
		if err != nil {
			if errors.Is(err, rlp.EOL) {
				break
			}
			f.logger.Error("[txpool.fetch] Failed to read ETH/63 tx raw RLP", "err", err, "peer_id", peerHex)
			return fmt.Errorf("decode ETH/63 tx raw: %w", err)
		}
		rawTxs = append(rawTxs, raw)
	}

	if err := s.ListEnd(); err != nil {
		f.logger.Error("[txpool.fetch] Failed to close ETH/63 transactions list", "err", err, "peer_id", peerHex)
		return fmt.Errorf("decode ETH/63 tx list end: %w", err)
	}

	if len(rawTxs) == 0 {
		f.logger.Info("[txpool.fetch] No transactions in ETH/63 message")
		return nil
	}

	// Decode using erigon-lib/types (transaction.go) for visibility/debugging.
	// Note: txpool parsing below is still the source of truth for slot classification.
	if dbg.Enabled(ctx) {
		for i, raw := range rawTxs {
			txObj, decErr := types.DecodeTransaction(raw)
			if decErr != nil {
				f.logger.Warn("[txpool.fetch] types.DecodeTransaction failed for ETH/63 tx",
					"peer_id", peerHex, "index", i, "err", decErr)
				continue
			}
			f.logger.Debug("[txpool.fetch] ETH/63 tx decoded via erigon-lib/types",
				"peer_id", peerHex, "index", i, "hash", txObj.Hash().Hex(), "type", txObj.Type())
		}
	}

	f.logger.Info("[txpool.fetch] Decoded ETH/63 transactions",
		"count", len(rawTxs),
		"peer_id", peerHex)

	// Convert ETH/63 transactions to TxnSlots
	txns := TxnSlots{}
	if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
		txns.Resize(uint(len(rawTxs)))

		for i, txRlp := range rawTxs {

			// Initialize TxnSlot
			txns.Txns[i] = &TxnSlot{}

			// Parse the RLP into TxnSlot
			// ETH/63 transactions are legacy format (no envelope, no blobs)
			_, err = parseContext.ParseTransaction(txRlp, 0, txns.Txns[i], txns.Senders.At(i),
				false /* hasEnvelope */, false, /* wrappedWithBlobs */
				func(hash []byte) error {
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
					f.logger.Debug("[txpool.fetch] ETH/63 transaction already known",
						"index", i)
					txns.Resize(uint(i))
					return nil
				}
				f.logger.Error("[txpool.fetch] Failed to parse ETH/63 transaction",
					"index", i, "err", err)
				txns.Resize(uint(i))
				return err
			}

			// Log transaction details (structured)
			txTypeName := f.getTransactionTypeName(txns.Txns[i].Type)
			toStr := "<contract_creation>"
			if txns.Txns[i].Creation == false {
				// Note: TxnSlot doesn't carry To address; keep placeholder to avoid wrong info
				toStr = "<unknown>"
			}
			f.logger.Info("[p2p.txpool] received transaction (eth/63)",
				"peer_id", peerHex,
				"tx_index", i,
				"tx_hash", fmt.Sprintf("%x", txns.Txns[i].IDHash[:]),
				"tx_type", txTypeName,
				"tx_type_num", txns.Txns[i].Type,
				"from", fmt.Sprintf("%x", txns.Senders.At(i)),
				"to", toStr,
				"nonce", txns.Txns[i].Nonce,
				"gas", txns.Txns[i].Gas,
				"value", &txns.Txns[i].Value,
				"gas_price", &txns.Txns[i].Tip, // for legacy Tip==GasPrice
				"tip_cap", &txns.Txns[i].Tip,
				"fee_cap", &txns.Txns[i].FeeCap,
			)

			// Validate: ETH/63 transactions MUST be legacy type
			if txns.Txns[i].Type != LegacyTxnType {
				f.logger.Warn("[txpool.fetch] ETH/63 transaction incorrectly classified",
					"index", i,
					"parsed_type", txns.Txns[i].Type,
					"expected_type", LegacyTxnType)
			}

			// Mark as remote transaction
			txns.IsLocal[i] = false
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
	f.logger.Info("[txpool.fetch] Added ETH/63 transactions to pool",
		"peer_id", peerHex,
		"added_count", len(txns.Txns))

	return nil
}

// getTransactionTypeName returns a human-readable name for transaction type
func (f *Fetch) getTransactionTypeName(txType byte) string {
	switch txType {
	case LegacyTxnType:
		return "legacy"
	case AccessListTxnType:
		return "access_list"
	case DynamicFeeTxnType:
		return "eip1559"
	case BlobTxnType:
		return "blob"
	case SetCodeTxnType:
		return "set_code"
	case AATxnType:
		return "account_abstraction"
	default:
		return "unknown"
	}
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
