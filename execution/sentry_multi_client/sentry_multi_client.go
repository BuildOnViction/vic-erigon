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

package sentry_multi_client

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/direct"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	libsentry "github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon-p2p/sentry"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/stages/bodydownload"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

// StartStreamLoops starts message processing loops for all sentries.
// The processing happens in several streams:
// RecvMessage - processing incoming headers/bodies
// RecvUploadMessage - sending bodies/receipts - may be heavy, it's ok to not process this messages enough fast, it's also ok to drop some of these messages if we can't process.
// RecvUploadHeadersMessage - sending headers - dedicated stream because headers propagation speed important for network health
// PeerEventsLoop - logging peer connect/disconnect events
func (cs *MultiClient) StartStreamLoops(ctx context.Context) {
	sentries := cs.Sentries()
	for i := range sentries {
		sentry := sentries[i]
		cs.logger.Debug("Starting stream loops for sentry", "sentryIndex", i)
		go cs.RecvMessageLoop(ctx, sentry, nil)
		go cs.RecvUploadMessageLoop(ctx, sentry, nil)
		go cs.RecvUploadHeadersMessageLoop(ctx, sentry, nil)
		go cs.PeerEventsLoop(ctx, sentry, nil)
	}
}

func (cs *MultiClient) RecvUploadMessageLoop(
	ctx context.Context,
	sentry proto_sentry.SentryClient,
	wg *sync.WaitGroup,
) {
	protocol := detectSentryProtocol(ctx, sentry)
	var messageIds []proto_sentry.MessageId

	switch protocol {
	case proto_sentry.Protocol_ETH63:
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH63][eth.GetBlockBodiesMsg],
			eth.ToProto[direct.ETH63][eth.GetReceiptsMsg],
		}
		streamFactory63 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadMessage/eth63", streamFactory63, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)

	default:
		// Default to ETH/67 for other protocols
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH67][eth.GetBlockBodiesMsg],
			eth.ToProto[direct.ETH67][eth.GetReceiptsMsg],
		}
		streamFactory67 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadMessage/eth67", streamFactory67, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
	}
}

func (cs *MultiClient) RecvUploadHeadersMessageLoop(
	ctx context.Context,
	sentry proto_sentry.SentryClient,
	wg *sync.WaitGroup,
) {
	protocol := detectSentryProtocol(ctx, sentry)
	var messageIds []proto_sentry.MessageId

	switch protocol {
	case proto_sentry.Protocol_ETH63:
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH63][eth.GetBlockHeadersMsg],
		}
		streamFactory63 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadHeadersMessage/eth63", streamFactory63, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)

	default:
		// Default to ETH/67 for other protocols
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH67][eth.GetBlockHeadersMsg],
		}
		streamFactory67 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadHeadersMessage/eth67", streamFactory67, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
	}
}

func (cs *MultiClient) RecvMessageLoop(
	ctx context.Context,
	sentry proto_sentry.SentryClient,
	wg *sync.WaitGroup,
) {
	protocol := detectSentryProtocol(ctx, sentry)
	cs.logger.Info("[P2P Sync] RecvMessageLoop: Starting message loop",
		"detectedProtocol", protocol.String())
	var messageIds []proto_sentry.MessageId

	switch protocol {
	case proto_sentry.Protocol_ETH63:
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH63][eth.BlockHeadersMsg],
			eth.ToProto[direct.ETH63][eth.BlockBodiesMsg],
			eth.ToProto[direct.ETH63][eth.NewBlockHashesMsg],
			eth.ToProto[direct.ETH63][eth.NewBlockMsg],
		}
		cs.logger.Info("[P2P Sync] RecvMessageLoop: Detected ETH/63 protocol, subscribing to messages",
			"messageIds", fmt.Sprintf("%v", messageIds),
			"count", len(messageIds))
		streamFactory63 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			cs.logger.Info("[P2P Sync] RecvMessageLoop: Requesting ETH/63 message stream",
				"messageCount", len(messageIds))
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvMessage/eth63", streamFactory63, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)

	default:
		// Default to ETH/67 for other protocols
		messageIds = []proto_sentry.MessageId{
			eth.ToProto[direct.ETH67][eth.BlockHeadersMsg],
			eth.ToProto[direct.ETH67][eth.BlockBodiesMsg],
			eth.ToProto[direct.ETH67][eth.NewBlockHashesMsg],
			eth.ToProto[direct.ETH67][eth.NewBlockMsg],
		}
		cs.logger.Info("[P2P Sync] RecvMessageLoop: Detected non-ETH/63 protocol, using ETH/67",
			"detectedProtocol", protocol.String(),
			"messageIds", fmt.Sprintf("%v", messageIds),
			"count", len(messageIds))
		streamFactory67 := func(streamCtx context.Context, s proto_sentry.SentryClient) (grpc.ClientStream, error) {
			cs.logger.Info("[P2P Sync] RecvMessageLoop: Requesting ETH/67 message stream",
				"messageCount", len(messageIds))
			return s.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: messageIds}, grpc.WaitForReady(true))
		}
		go libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvMessage/eth67", streamFactory67, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
	}
}

func (cs *MultiClient) PeerEventsLoop(
	ctx context.Context,
	sentry proto_sentry.SentryClient,
	wg *sync.WaitGroup,
) {
	streamFactory := func(streamCtx context.Context, sentry proto_sentry.SentryClient) (grpc.ClientStream, error) {
		return sentry.PeerEvents(streamCtx, &proto_sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}
	messageFactory := func() *proto_sentry.PeerEvent {
		return new(proto_sentry.PeerEvent)
	}

	libsentry.ReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "PeerEvents", streamFactory, messageFactory, cs.HandlePeerEvent, wg, cs.logger)
}

// MultiClient - does handle request/response/subscriptions to multiple sentries
// each sentry may support same or different p2p protocol
type MultiClient struct {
	Hd                                *headerdownload.HeaderDownload
	Bd                                *bodydownload.BodyDownload
	IsMock                            bool
	sentries                          []proto_sentry.SentryClient
	ChainConfig                       *chain.Config
	db                                kv.TemporalRoDB
	Engine                            consensus.Engine
	blockReader                       services.FullBlockReader
	statusDataProvider                *sentry.StatusDataProvider
	logPeerInfo                       bool
	sendHeaderRequestsToMultiplePeers bool
	maxBlockBroadcastPeers            func(*types.Header) uint

	// disableBlockDownload is meant to be used temporarily for astrid until work to
	// decouple sentry multi client from header and body downloading logic is done
	disableBlockDownload bool

	logger                           log.Logger
	getReceiptsActiveGoroutineNumber *semaphore.Weighted
	ethApiWrapper                    eth.ReceiptsGetter
}

var _ eth.ReceiptsGetter = new(receipts.Generator) // compile-time interface-check

func NewMultiClient(
	db kv.TemporalRoDB,
	chainConfig *chain.Config,
	engine consensus.Engine,
	sentries []proto_sentry.SentryClient,
	syncCfg ethconfig.Sync,
	blockReader services.FullBlockReader,
	blockBufferSize int,
	statusDataProvider *sentry.StatusDataProvider,
	logPeerInfo bool,
	maxBlockBroadcastPeers func(*types.Header) uint,
	disableBlockDownload bool,
	logger log.Logger,
) (*MultiClient, error) {
	// header downloader
	var hd *headerdownload.HeaderDownload
	if !disableBlockDownload {
		hd = headerdownload.NewHeaderDownload(
			512,       /* anchorLimit */
			1024*1024, /* linkLimit */
			engine,
			blockReader,
			logger,
		)
		if chainConfig.TerminalTotalDifficultyPassed {
			hd.SetPOSSync(true)
		}
		if err := hd.RecoverFromDb(db); err != nil {
			return nil, fmt.Errorf("recovery from DB failed: %w", err)
		}
	} else {
		hd = &headerdownload.HeaderDownload{}
	}

	// body downloader
	var bd *bodydownload.BodyDownload
	if !disableBlockDownload {
		bd = bodydownload.NewBodyDownload(engine, blockBufferSize, int(syncCfg.BodyCacheLimit), blockReader, logger)
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			return bd.UpdateFromDb(tx)
		}); err != nil {
			return nil, err
		}
	} else {
		bd = &bodydownload.BodyDownload{}
	}

	cs := &MultiClient{
		Hd:                                hd,
		Bd:                                bd,
		sentries:                          sentries,
		ChainConfig:                       chainConfig,
		db:                                db,
		Engine:                            engine,
		blockReader:                       blockReader,
		statusDataProvider:                statusDataProvider,
		logPeerInfo:                       logPeerInfo,
		sendHeaderRequestsToMultiplePeers: chainConfig.TerminalTotalDifficultyPassed,
		maxBlockBroadcastPeers:            maxBlockBroadcastPeers,
		disableBlockDownload:              disableBlockDownload,
		logger:                            logger,
		getReceiptsActiveGoroutineNumber:  semaphore.NewWeighted(1),
		ethApiWrapper:                     receipts.NewGenerator(blockReader, engine),
	}

	return cs, nil
}

func (cs *MultiClient) Sentries() []proto_sentry.SentryClient { return cs.sentries }

func (cs *MultiClient) newBlockHashes66(ctx context.Context, req *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	if cs.Hd.InitialCycle() && !cs.Hd.FetchingNew() {
		return nil
	}
	//cs.logger.Info(fmt.Sprintf("NewBlockHashes from [%s]", ConvertH256ToPeerID(req.PeerId)))
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes66: %w", err)
	}
	for _, announce := range request {
		cs.Hd.SaveExternalAnnounce(announce.Hash)
		if cs.Hd.HasLink(announce.Hash) {
			continue
		}
		b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
			RequestId: rand.Uint64(), // nolint: gosec
			GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
				Amount:  1,
				Reverse: false,
				Skip:    0,
				Origin:  eth.HashOrNumber{Hash: announce.Hash},
			},
		})
		if err != nil {
			return fmt.Errorf("encode header request: %w", err)
		}
		outreq := proto_sentry.SendMessageByIdRequest{
			PeerId: req.PeerId,
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: b,
			},
		}

		if _, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			return fmt.Errorf("send header request: %w", err)
		}
	}
	return nil
}

func (cs *MultiClient) blockHeaders66(ctx context.Context, in *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	// Parse the entire packet from scratch
	var pkt eth.BlockHeadersPacket66
	if err := rlp.DecodeBytes(in.Data, &pkt); err != nil {
		cs.logger.Error("[P2P] Failed to decode ETH/66 BlockHeadersPacket66", "err", err, "dataLen", len(in.Data))
		return fmt.Errorf("decode BlockHeadersPacket66: %w", err)
	}

	// Log PoSV header decoding verification
	cs.logPoSVHeaderDecoding(pkt.BlockHeadersPacket, "ETH/66")

	// Prepare to extract raw headers from the stream
	rlpStream := rlp.NewStream(bytes.NewReader(in.Data), uint64(len(in.Data)))
	if _, err := rlpStream.List(); err != nil {
		return fmt.Errorf("decode BlockHeadersPacket66 list: %w", err)
	}
	if _, err := rlpStream.Uint(); err != nil {
		return fmt.Errorf("decode BlockHeadersPacket66 requestID: %w", err)
	}
	// Now stream is at the BlockHeadersPacket, which is list of headers

	return cs.blockHeaders(ctx, pkt.BlockHeadersPacket, rlpStream, in.PeerId, sentry)
}

func (cs *MultiClient) blockHeaders(ctx context.Context, pkt eth.BlockHeadersPacket, rlpStream *rlp.Stream, peerID *proto_types.H512, sentryClient proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	if len(pkt) == 0 {
		return nil
	}

	// Stream is at the BlockHeadersPacket, which is list of headers
	if _, err := rlpStream.List(); err != nil {
		return fmt.Errorf("decode BlockHeadersPacket list: %w", err)
	}

	// Log PoSV header decoding verification
	cs.logPoSVHeaderDecoding(pkt, "ETH/66")

	// Process headers using common logic (extract raw RLP from stream)
	return cs.processHeaders(ctx, pkt, rlpStream, peerID, sentryClient, false /* encodeHeaders */)
}

func (cs *MultiClient) newBlock66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	// Parse the entire request from scratch
	request := &eth.NewBlockPacket{}
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockPacket66: %w", err)
	}

	return cs.processNewBlock(ctx, request, inreq.PeerId, sentryClient)
}

// processNewBlock processes a new block announcement (common logic for ETH/63 and ETH/66)
func (cs *MultiClient) processNewBlock(
	ctx context.Context,
	request *eth.NewBlockPacket,
	peerID *proto_types.H512,
	sentryClient proto_sentry.SentryClient,
) error {
	if err := request.SanityCheck(); err != nil {
		return fmt.Errorf("newBlock sanity check: %w", err)
	}
	if err := request.Block.HashCheck(true); err != nil {
		return fmt.Errorf("newBlock hash check: %w", err)
	}

	// Extract header raw bytes
	headerRaw, err := rlp.EncodeToBytes(request.Block.Header())
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	segments, penalty, err := cs.Hd.SingleHeaderAsSegment(headerRaw, request.Block.Header(), true /* penalizePoSBlocks */)
	if err != nil {
		return fmt.Errorf("singleHeaderAsSegment failed: %w", err)
	}

	if penalty == headerdownload.NoPenalty {
		propagate := !cs.ChainConfig.TerminalTotalDifficultyPassed
		// Do not propagate blocks who are post TTD
		firstPosSeen := cs.Hd.FirstPoSHeight()
		if firstPosSeen != nil && propagate {
			propagate = *firstPosSeen >= segments[0].Number
		}
		if !cs.IsMock && propagate {
			cs.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
				{
					Number: segments[0].Number,
					Hash:   segments[0].Hash,
				},
			})
		}

		cs.Hd.ProcessHeaders(segments, true /* newBlock */, sentry.ConvertH512ToPeerID(peerID))
	} else {
		cs.penalizePeer(ctx, peerID)
	}

	cs.Bd.AddToPrefetch(request.Block.Header(), request.Block.RawBody())
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   peerID,
		MinBlock: request.Block.NumberU64(),
	}
	if _, err := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
		cs.logger.Error("Could not send min block for peer", "err", err)
	}

	cs.logger.Trace(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), sentry.ConvertH512ToPeerID(peerID)))
	return nil
}

// penalizePeer penalizes a peer for sending invalid data
func (cs *MultiClient) penalizePeer(ctx context.Context, peerID *proto_types.H512) {
	outreq := proto_sentry.PenalizePeerRequest{
		PeerId:  peerID,
		Penalty: proto_sentry.PenaltyKind_Kick,
	}
	for _, sentry := range cs.sentries {
		if directSentry, ok := sentry.(direct.SentryClient); ok && !directSentry.Ready() {
			continue
		}
		if _, err := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
			cs.logger.Error("Could not send penalty", "err", err)
		}
	}
}

func (cs *MultiClient) blockBodies66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	var request eth.BlockRawBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket66: %w", err)
	}
	txs, uncles, withdrawals := request.BlockRawBodiesPacket.Unpack()
	if len(txs) == 0 && len(uncles) == 0 && len(withdrawals) == 0 {
		// No point processing empty response
		return nil
	}
	cs.Bd.DeliverBodies(txs, uncles, withdrawals, uint64(len(inreq.Data)), sentry.ConvertH512ToPeerID(inreq.PeerId))
	return nil
}

func (cs *MultiClient) receipts66(_ context.Context, _ *proto_sentry.InboundMessage, _ proto_sentry.SentryClient) error {
	return nil
}

func (cs *MultiClient) getBlockHeaders66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockHeadersPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockHeaders66: %w, data: %x", err, inreq.Data)
	}

	var headers []*types.Header
	if err := cs.db.View(ctx, func(tx kv.Tx) (err error) {
		headers, err = eth.AnswerGetBlockHeadersQuery(tx, query.GetBlockHeadersPacket, cs.blockReader)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}

	// Even if we get empty headers list from db, we'll respond with that. Nodes
	// running on erigon 2.48 with --sentry.drop-useless-peers will kick us out
	// because of certain checks. But, nodes post that will not kick us out. This
	// is useful as currently with no response, we're anyways getting kicked due
	// to request timeout and EOF.

	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          query.RequestId,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_HEADERS_66,
			Data: b,
		},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil && !isPeerNotFoundErr(err) {
		return fmt.Errorf("send header response 66: %w", err)
	}
	//cs.logger.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t, responseLen=%d}", ConvertH512ToPeerID(inreq.PeerId), query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse, len(b)))
	return nil
}

func (cs *MultiClient) getBlockBodies66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockBodies66: %w, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, query.GetBlockBodiesPacket, cs.blockReader)
	tx.Rollback()
	b, err := rlp.EncodeToBytes(&eth.BlockBodiesRLPPacket66{
		RequestId:            query.RequestId,
		BlockBodiesRLPPacket: response,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_BODIES_66,
			Data: b,
		},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		if isPeerNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("send bodies response: %w", err)
	}
	//cs.logger.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg responseLen %d", ConvertH512ToPeerID(inreq.PeerId), len(b)))
	return nil
}

func (cs *MultiClient) getReceipts66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient proto_sentry.SentryClient) error {
	var query eth.GetReceiptsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getReceipts66: %w, data: %x", err, inreq.Data)
	}
	cachedReceipts, needMore, err := eth.AnswerGetReceiptsQueryCacheOnly(ctx, cs.ethApiWrapper, query.GetReceiptsPacket)
	if err != nil {
		return err
	}
	receiptsList := []rlp.RawValue{}
	if cachedReceipts != nil {
		receiptsList = cachedReceipts.EncodedReceipts
	}
	if needMore {
		err = cs.getReceiptsActiveGoroutineNumber.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		defer cs.getReceiptsActiveGoroutineNumber.Release(1)

		tx, err := cs.db.BeginTemporalRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		receiptsList, err = eth.AnswerGetReceiptsQuery(ctx, cs.ChainConfig, cs.ethApiWrapper, cs.blockReader, tx, query.GetReceiptsPacket, cachedReceipts)
		if err != nil {
			return err
		}

	}
	b, err := rlp.EncodeToBytes(&eth.ReceiptsRLPPacket66{
		RequestId:         query.RequestId,
		ReceiptsRLPPacket: receiptsList,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_RECEIPTS_66,
			Data: b,
		},
	}
	_, err = sentryClient.SendMessageById(ctx, &outreq, &grpc.OnFinishCallOption{})
	if err != nil {
		if isPeerNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("send receipts response: %w", err)
	}
	//println(fmt.Sprintf("[%s] GetReceipts responseLen %d", sentry.ConvertH512ToPeerID(inreq.PeerId), len(b)))
	return nil
}

func MakeInboundMessage() *proto_sentry.InboundMessage {
	return new(proto_sentry.InboundMessage)
}

func (cs *MultiClient) HandleInboundMessage(ctx context.Context, message *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, msgID=%s, trace: %s", rec, message.Id.String(), dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	err = cs.handleInboundMessage(ctx, message, sentry)
	if (err != nil) && rlp.IsInvalidRLPError(err) {
		cs.logger.Debug("Kick peer for invalid RLP", "err", err)
		penalizeRequest := proto_sentry.PenalizePeerRequest{
			PeerId:  message.PeerId,
			Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		if _, err1 := sentry.PenalizePeer(ctx, &penalizeRequest, &grpc.EmptyCallOption{}); err1 != nil {
			cs.logger.Error("Could not send penalty", "err", err1)
		}
	}

	return err
}

func (cs *MultiClient) handleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	switch inreq.Id {
	// ========= eth 63 ==========
	case proto_sentry.MessageId_NEW_BLOCK_HASHES_63:
		return cs.newBlockHashes63(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_HEADERS_63:
		return cs.blockHeaders63(ctx, inreq, sentry)
	case proto_sentry.MessageId_NEW_BLOCK_63:
		return cs.newBlock63(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_BODIES_63:
		return cs.blockBodies63(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_HEADERS_63:
		return cs.getBlockHeaders63(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_BODIES_63:
		return cs.getBlockBodies63(ctx, inreq, sentry)
	case proto_sentry.MessageId_RECEIPTS_63:
		return cs.receipts63(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_RECEIPTS_63:
		return cs.getReceipts63(ctx, inreq, sentry)
	case proto_sentry.MessageId_TRANSACTIONS_63:
		return cs.transactions63(ctx, inreq, sentry)

	// ========= eth 67 ==========
	case proto_sentry.MessageId_NEW_BLOCK_HASHES_66:
		return cs.newBlockHashes66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_HEADERS_66:
		return cs.blockHeaders66(ctx, inreq, sentry)
	case proto_sentry.MessageId_NEW_BLOCK_66:
		return cs.newBlock66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_BODIES_66:
		return cs.blockBodies66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_HEADERS_66:
		return cs.getBlockHeaders66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_BODIES_66:
		return cs.getBlockBodies66(ctx, inreq, sentry)
	case proto_sentry.MessageId_RECEIPTS_66:
		return cs.receipts66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_RECEIPTS_66:
		return cs.getReceipts66(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (cs *MultiClient) HandlePeerEvent(ctx context.Context, event *proto_sentry.PeerEvent, sentryClient proto_sentry.SentryClient) error {
	eventID := event.EventId.String()
	peerID := sentry.ConvertH512ToPeerID(event.PeerId)
	peerIDStr := hex.EncodeToString(peerID[:])

	if !cs.logPeerInfo {
		cs.logger.Debug("[p2p] Sentry peer did", "eventID", eventID, "peer", peerIDStr)
		return nil
	}

	var nodeURL string
	var clientID string
	var capabilities []string
	if event.EventId == proto_sentry.PeerEvent_Connect {
		reply, err := sentryClient.PeerById(ctx, &proto_sentry.PeerByIdRequest{PeerId: event.PeerId})
		if err != nil {
			cs.logger.Debug("sentry.PeerById failed", "err", err)
		}
		if (reply != nil) && (reply.Peer != nil) {
			nodeURL = reply.Peer.Enode
			clientID = reply.Peer.Name
			capabilities = reply.Peer.Caps
		}
	}

	cs.logger.Debug("[p2p] Sentry peer did", "eventID", eventID, "peer", peerIDStr,
		"nodeURL", nodeURL, "clientID", clientID, "capabilities", capabilities)
	return nil
}

func (cs *MultiClient) makeStatusData(ctx context.Context) (*proto_sentry.StatusData, error) {
	return cs.statusDataProvider.GetStatusData(ctx)
}

func GrpcClient(ctx context.Context, sentryAddr string) (*direct.SentryClientRemote, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.DialContext(ctx, sentryAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return direct.NewSentryClientRemote(proto_sentry.NewSentryClient(conn)), nil
}

// ETH63 helper functions
func (cs *MultiClient) getBlockHeaders(ctx context.Context, query eth.GetBlockHeadersPacket, peerID *proto_types.H512, sentryClient proto_sentry.SentryClient) error {
	cs.logger.Trace("Querying DB for headers", "originNumber", query.Origin.Number, "originHash", query.Origin.Hash.Hex(), "amount", query.Amount)
	var headers []*types.Header
	if err := cs.db.View(ctx, func(tx kv.Tx) (err error) {
		headers, err = eth.AnswerGetBlockHeadersQuery(tx, &query, cs.blockReader)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}

	// Encode ETH63 response (no request ID)
	headersPacket := eth.BlockHeadersPacket(headers)
	b, err := rlp.EncodeToBytes(&headersPacket)
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: peerID,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_HEADERS_63,
			Data: b,
		},
	}
	_, err = sentryClient.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil && !isPeerNotFoundErr(err) {
		return fmt.Errorf("send header response 63: %w", err)
	}
	cs.logger.Trace("Sent BLOCK_HEADERS_63 response", "headerCount", len(headers), "dataSize", len(b))
	return nil
}

func (cs *MultiClient) getBlockBodies(ctx context.Context, query eth.GetBlockBodiesPacket, peerID *proto_types.H512, sentryClient proto_sentry.SentryClient) error {
	tx, err := cs.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, query, cs.blockReader)
	tx.Rollback()

	// Encode ETH63 response (no request ID)
	bodiesPacket := eth.BlockBodiesRLPPacket(response)
	b, err := rlp.EncodeToBytes(&bodiesPacket)
	if err != nil {
		return fmt.Errorf("encode bodies response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: peerID,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_BODIES_63,
			Data: b,
		},
	}
	_, err = sentryClient.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		if isPeerNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("send bodies response: %w", err)
	}
	return nil
}

func (cs *MultiClient) getReceipts(ctx context.Context, query eth.GetReceiptsPacket, peerID *proto_types.H512, sentryClient proto_sentry.SentryClient) error {
	cachedReceipts, needMore, err := eth.AnswerGetReceiptsQueryCacheOnly(ctx, cs.ethApiWrapper, query)
	if err != nil {
		return err
	}
	receiptsList := []rlp.RawValue{}
	if cachedReceipts != nil {
		receiptsList = cachedReceipts.EncodedReceipts
	}
	if needMore {
		err = cs.getReceiptsActiveGoroutineNumber.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		defer cs.getReceiptsActiveGoroutineNumber.Release(1)

		tx, err := cs.db.BeginTemporalRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		receiptsList, err = eth.AnswerGetReceiptsQuery(ctx, cs.ChainConfig, cs.ethApiWrapper, cs.blockReader, tx, query, cachedReceipts)
		if err != nil {
			return err
		}
	}

	// Encode ETH63 response (no request ID)
	receiptsPacket := eth.ReceiptsRLPPacket(receiptsList)
	b, err := rlp.EncodeToBytes(&receiptsPacket)
	if err != nil {
		return fmt.Errorf("encode receipts response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: peerID,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_RECEIPTS_63,
			Data: b,
		},
	}
	_, err = sentryClient.SendMessageById(ctx, &outreq, &grpc.OnFinishCallOption{})
	if err != nil {
		if isPeerNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("send receipts response: %w", err)
	}
	return nil
}

func (cs *MultiClient) newBlockHashes63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	return cs.newBlockHashes66(ctx, inreq, sentry) // Same implementation as ETH66
}

func (cs *MultiClient) blockHeaders63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	// Parse ETH/63 block headers (no request ID)
	// The RLP decoding uses Header.DecodeRLP which correctly handles PoSV fields:
	// - Validators (from Geth) -> NewAttestors
	// - Validator (from Geth) -> Attestor
	// - Penalties (from Geth) -> Penalties
	var pkt eth.BlockHeadersPacket
	if err := rlp.DecodeBytes(inreq.Data, &pkt); err != nil {
		cs.logger.Error("[P2P] Failed to decode ETH/63 BlockHeadersPacket", "err", err, "dataLen", len(inreq.Data))
		return fmt.Errorf("decode BlockHeadersPacket63: %w", err)
	}

	// Log PoSV header decoding verification for block 900
	cs.logPoSVHeaderDecoding(pkt, "ETH/63")

	// Convert headers to ChainSegmentHeader format and process
	return cs.processHeaders(ctx, pkt, nil, inreq.PeerId, sentry, true /* encodeHeaders */)
}

// logPoSVHeaderDecoding logs PoSV header decoding verification for debugging
func (cs *MultiClient) logPoSVHeaderDecoding(headers []*types.Header, protocol string) {
	for _, header := range headers {
		if header.Number.Uint64() == 900 {
			expectedHash := common.HexToHash("0xe04d1f1a02963603361e86975dbf23e7c4755da14192086694a72e9096eb449a")
			expectedValidators, _ := hex.DecodeString("000000330000003000000035000000320000003100000034")
			cs.logger.Info("[P2P] Decoded block 900",
				"protocol", protocol,
				"hash", header.Hash().Hex(),
				"expectedHash", expectedHash.Hex(),
				"hashMatch", header.Hash() == expectedHash,
				"posv", header.Posv,
				"validatorsLen", len(header.NewAttestors),
				"validatorsMatch", bytes.Equal(header.NewAttestors, expectedValidators),
				"validatorLen", len(header.Attestor),
				"penaltiesLen", len(header.Penalties))
		}
	}
}

// convertHeadersToChainSegmentHeaders converts headers to ChainSegmentHeader format
// If rlpStream is provided, it extracts raw RLP from the stream. Otherwise, it encodes headers.
func (cs *MultiClient) convertHeadersToChainSegmentHeaders(
	headers []*types.Header,
	rlpStream *rlp.Stream,
	encodeHeaders bool,
) ([]headerdownload.ChainSegmentHeader, uint64, error) {
	if len(headers) == 0 {
		return nil, 0, nil
	}

	csHeaders := make([]headerdownload.ChainSegmentHeader, 0, len(headers))
	var highestBlock uint64

	for _, header := range headers {
		var headerRaw []byte
		var err error

		if rlpStream != nil {
			// Extract raw RLP from stream (for ETH/66)
			headerRaw, err = rlpStream.Raw()
			if err != nil {
				return nil, 0, fmt.Errorf("extract raw RLP: %w", err)
			}
			headerRaw = append([]byte{}, headerRaw...)
		} else if encodeHeaders {
			// Encode header to get raw bytes (for ETH/63)
			headerRaw, err = rlp.EncodeToBytes(header)
			if err != nil {
				return nil, 0, fmt.Errorf("encode header: %w", err)
			}
		} else {
			// Use header hash as raw (fallback)
			headerRaw = header.Hash().Bytes()
		}

		number := header.Number.Uint64()
		if number > highestBlock {
			highestBlock = number
		}

		hash := header.Hash()
		if rlpStream != nil {
			// For ETH/66, verify hash matches raw RLP
			hash = types.RawRlpHash(headerRaw)
		}

		csHeaders = append(csHeaders, headerdownload.ChainSegmentHeader{
			Header:    header,
			HeaderRaw: headerRaw,
			Hash:      hash,
			Number:    number,
		})
	}

	return csHeaders, highestBlock, nil
}

// processHeaders processes headers through the downloader (common logic for ETH/63 and ETH/66)
func (cs *MultiClient) processHeaders(
	ctx context.Context,
	headers []*types.Header,
	rlpStream *rlp.Stream,
	peerID *proto_types.H512,
	sentryClient proto_sentry.SentryClient,
	encodeHeaders bool,
) error {
	if len(headers) == 0 {
		return nil
	}

	// Convert headers to ChainSegmentHeader format
	csHeaders, highestBlock, err := cs.convertHeadersToChainSegmentHeaders(headers, rlpStream, encodeHeaders)
	if err != nil {
		return err
	}

	// Process headers through the downloader
	if cs.Hd.POSSync() {
		sort.Sort(headerdownload.HeadersReverseSort(csHeaders))
		tx, err := cs.db.BeginTemporalRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		penalties, err := cs.Hd.ProcessHeadersPOS(csHeaders, tx, sentry.ConvertH512ToPeerID(peerID))
		if err != nil {
			return err
		}
		if len(penalties) > 0 {
			cs.Penalize(ctx, penalties)
		}
	} else {
		sort.Sort(headerdownload.HeadersSort(csHeaders))
		canRequestMore := cs.Hd.ProcessHeaders(csHeaders, false /* newBlock */, sentry.ConvertH512ToPeerID(peerID))

		if canRequestMore {
			currentTime := time.Now()
			cs.handleMoreHeadersRequest(ctx, currentTime)
		}
	}

	// Update peer min block
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   peerID,
		MinBlock: highestBlock,
	}
	if _, err := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
		cs.logger.Error("Could not send min block for peer", "err", err)
	}

	return nil
}

// handleMoreHeadersRequest handles requesting more headers when needed
func (cs *MultiClient) handleMoreHeadersRequest(ctx context.Context, currentTime time.Time) {
	req, penalties := cs.Hd.RequestMoreHeaders(currentTime)
	if req != nil {
		if peer, sentToPeer := cs.SendHeaderRequest(ctx, req); sentToPeer {
			cs.Hd.UpdateStats(req, false /* skeleton */, peer)
			cs.Hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
		}
	}
	if len(penalties) > 0 {
		cs.Penalize(ctx, penalties)
	}
}

func (cs *MultiClient) newBlock63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	// Parse ETH/63 NewBlock packet (no request ID)
	var request eth.NewBlockPacket
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockPacket63: %w", err)
	}

	return cs.processNewBlock(ctx, &request, inreq.PeerId, sentryClient)
}

func (cs *MultiClient) blockBodies63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient proto_sentry.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	// Parse ETH/63 BlockBodies packet (no request ID)
	var request eth.BlockRawBodiesPacket
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket63: %w", err)
	}

	txs, uncles, withdrawals := request.Unpack()

	if len(txs) == 0 && len(uncles) == 0 && len(withdrawals) == 0 {
		// No point processing empty response
		return nil
	}

	cs.logger.Trace("Decoded ETH/63 block bodies",
		"blockCount", len(txs),
		"dataSize", len(inreq.Data))

	cs.Bd.DeliverBodies(txs, uncles, withdrawals, uint64(len(inreq.Data)), sentry.ConvertH512ToPeerID(inreq.PeerId))
	return nil
}

func (cs *MultiClient) getBlockHeaders63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockHeadersPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockHeaders63: %w, data: %x", err, inreq.Data)
	}
	cs.logger.Trace("Received GET_BLOCK_HEADERS_63 request",
		"originHash", query.Origin.Hash.Hex(),
		"originNumber", query.Origin.Number,
		"amount", query.Amount,
		"skip", query.Skip,
		"reverse", query.Reverse)
	return cs.getBlockHeaders(ctx, query, inreq.PeerId, sentry)
}

func (cs *MultiClient) getBlockBodies63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockBodiesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockBodies63: %w, data: %x", err, inreq.Data)
	}
	return cs.getBlockBodies(ctx, query, inreq.PeerId, sentry)
}

func (cs *MultiClient) receipts63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	return cs.receipts66(ctx, inreq, sentry) // Same implementation as ETH66
}

func (cs *MultiClient) getReceipts63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetReceiptsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getReceipts63: %w, data: %x", err, inreq.Data)
	}
	return cs.getReceipts(ctx, query, inreq.PeerId, sentry)
}

func (cs *MultiClient) transactions63(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	// return cs.transactions66(ctx, inreq, sentry) // Same implementation as ETH66
	return nil
}
