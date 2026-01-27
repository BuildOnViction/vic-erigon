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
	"context"
	"fmt"
	"math/rand"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon-p2p/sentry"
	"github.com/erigontech/erigon/turbo/stages/bodydownload"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

// detectSentryProtocol detects the protocol version for a specific sentry client
func detectSentryProtocol(ctx context.Context, sentry proto_sentry.SentryClient) proto_sentry.Protocol {
	// Method 1: Try Protocol() method (returns uint)
	if protocolMethod, ok := sentry.(interface{ Protocol() uint }); ok {
		protocolUint := protocolMethod.Protocol()
		switch protocolUint {
		case 63:
			return proto_sentry.Protocol_ETH63
		case 67:
			return proto_sentry.Protocol_ETH67
		case 68:
			return proto_sentry.Protocol_ETH68
		default:
			return proto_sentry.Protocol_ETH67
		}
	}

	// Method 2: Try GetProtocol() method
	if protocolMethod, ok := sentry.(interface{ GetProtocol() proto_sentry.Protocol }); ok {
		return protocolMethod.GetProtocol()
	}

	// Method 3: Try context
	if ctxProtocol, ok := ctx.Value("protocol").(proto_sentry.Protocol); ok {
		return ctxProtocol
	}

	// Default to ETH/67
	return proto_sentry.Protocol_ETH67
}

// getPeerName attempts to get the peer name from sentry, returns "unknown" if unavailable
func (cs *MultiClient) getPeerName(ctx context.Context, sentry proto_sentry.SentryClient, peerID *proto_types.H512) string {
	peerInfoReply, err := sentry.PeerById(ctx, &proto_sentry.PeerByIdRequest{PeerId: peerID}, &grpc.EmptyCallOption{})
	if err == nil && peerInfoReply != nil && peerInfoReply.Peer != nil && peerInfoReply.Peer.Name != "" {
		return peerInfoReply.Peer.Name
	}
	return "unknown"
}

// Methods of sentry called by Core

func (cs *MultiClient) SetStatus(ctx context.Context) {
	statusMsg, err := cs.statusDataProvider.GetStatusData(ctx)
	if err != nil {
		cs.logger.Error("MultiClient.SetStatus: GetStatusData error", "err", err)
		return
	}

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			cs.logger.Error("Update status message for the sentry", "err", err)
		}
	}
}

func (cs *MultiClient) SendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) (peerID [64]byte, ok bool) {
	// Try each sentry until we find one that can handle the request
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		// Try all protocol versions in order: ETH/63, ETH/67, ETH/68
		// This ensures we use the protocol version that has available peers
		protocolsToTry := []proto_sentry.Protocol{
			proto_sentry.Protocol_ETH63,
			proto_sentry.Protocol_ETH67,
			proto_sentry.Protocol_ETH68,
		}

		var (
			bytes     []byte
			err       error
			messageId proto_sentry.MessageId
		)

		// Try each protocol version until one succeeds
		var sentPeers *proto_sentry.SentPeers
		var successfulProtocol proto_sentry.Protocol
		var successfulMessageId proto_sentry.MessageId

		for _, protocol := range protocolsToTry {
			// Use the appropriate message format based on protocol
			switch protocol {
			case proto_sentry.Protocol_ETH63:
				// For ETH/63, use the legacy format without request ID
				bytes, err = rlp.EncodeToBytes(eth.GetBlockBodiesPacket(req.Hashes))
				if err != nil {
					cs.logger.Warn("Could not encode ETH/63 body request", "err", err, "sentryIndex", i)
					continue
				}
				messageId = proto_sentry.MessageId_GET_BLOCK_BODIES_63

			case proto_sentry.Protocol_ETH67, proto_sentry.Protocol_ETH68:
				// For ETH/66+, use format with request ID
				bytes, err = rlp.EncodeToBytes(&eth.GetBlockBodiesPacket66{
					RequestId:            rand.Uint64(), // nolint: gosec
					GetBlockBodiesPacket: req.Hashes,
				})
				if err != nil {
					cs.logger.Warn("Could not encode ETH/66+ body request", "err", err, "sentryIndex", i, "protocol", protocol.String())
					continue
				}
				messageId = proto_sentry.MessageId_GET_BLOCK_BODIES_66

			default:
				continue
			}

			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: req.BlockNums[len(req.BlockNums)-1],
				Data: &proto_sentry.OutboundMessageData{
					Id:   messageId,
					Data: bytes,
				},
				MaxPeers: 1,
			}

			sentPeers, err = cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
			if err != nil {
				cs.logger.Warn("Could not send body request",
					"protocol", protocol.String(),
					"messageId", messageId.String(),
					"sentryIndex", i,
					"err", err)
				continue
			}

			// Check for empty peers - if we got peers, this protocol version works!
			if sentPeers != nil && len(sentPeers.Peers) > 0 {
				successfulProtocol = protocol
				successfulMessageId = messageId
				break // Found a working protocol version
			}
		}

		// If no protocol version succeeded, try next sentry
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}

		// Now safe to access sentPeers.Peers[0]
		peerID := sentry.ConvertH512ToPeerID(sentPeers.Peers[0])

		// Get peer name for logging
		peerName := cs.getPeerName(ctx, cs.sentries[i], sentPeers.Peers[0])

		cs.logger.Info("Sent body request to peer",
			"protocol", successfulProtocol.String(),
			"messageId", successfulMessageId.String(),
			"peer", fmt.Sprintf("%x", peerID[:8]),
			"peerName", peerName,
			"sentryIndex", i,
			"blockCount", len(req.BlockNums))
		return peerID, true
	}

	return [64]byte{}, false
}

func (cs *MultiClient) SendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) (peerID [64]byte, ok bool) {
	// If hash is zero but we have a number, try to look up the hash from database
	hash := req.Hash
	if hash == (common.Hash{}) && req.Number > 0 {
		err := cs.db.View(ctx, func(tx kv.Tx) error {
			var found bool
			var err error
			hash, found, err = cs.blockReader.CanonicalHash(ctx, tx, req.Number)
			if err != nil {
				return err
			}
			_ = found // Hash lookup result (used for validation if needed)
			return nil
		})
		if err != nil {
			cs.logger.Warn("[SendHeaderRequest] Failed to look up hash for block number",
				"number", req.Number,
				"err", err,
				"note", "Request will be sent as number-based")
		}
	}

	// Try each sentry until we find one that can handle the request
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		// Try all protocol versions in order: ETH/63, ETH/67, ETH/68
		// This ensures we use the protocol version that has available peers
		protocolsToTry := []proto_sentry.Protocol{
			proto_sentry.Protocol_ETH63,
			proto_sentry.Protocol_ETH67,
			proto_sentry.Protocol_ETH68,
		}

		var (
			bytes     []byte
			err       error
			messageId proto_sentry.MessageId
			maxPeers  int32 = 5 // Default number of peers to try
		)

		// Create the request data
		requestID := rand.Uint64()

		// Try each protocol version until one succeeds
		var sentPeers *proto_sentry.SentPeers
		// var successfulProtocol proto_sentry.Protocol
		// var successfulMessageId proto_sentry.MessageId

		for _, protocol := range protocolsToTry {
			// Use the appropriate message format based on protocol
			switch protocol {
			case proto_sentry.Protocol_ETH63:
				// For ETH/63, use the legacy format without request ID
				req63 := &eth.GetBlockHeadersPacket{
					Origin: eth.HashOrNumber{
						Hash:   hash,
						Number: 0, // Must be 0 when hash is provided
					},
					Amount:  req.Length,
					Skip:    req.Skip,
					Reverse: req.Reverse,
				}
				// If hash is zero, use number-based request
				if hash == (common.Hash{}) {
					req63.Origin.Number = req.Number
					req63.Origin.Hash = common.Hash{}
				}
				bytes, err = rlp.EncodeToBytes(req63)
				if err != nil {
					cs.logger.Warn("Could not encode ETH/63 header request", "err", err, "sentryIndex", i)
					continue
				}
				messageId = proto_sentry.MessageId_GET_BLOCK_HEADERS_63

			case proto_sentry.Protocol_ETH67, proto_sentry.Protocol_ETH68:
				// For ETH/66+, use format with request ID
				req66 := &eth.GetBlockHeadersPacket66{
					RequestId: requestID,
					GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
						Origin: eth.HashOrNumber{
							Hash:   hash,
							Number: 0, // Must be 0 when hash is provided
						},
						Amount:  req.Length,
						Skip:    req.Skip,
						Reverse: req.Reverse,
					},
				}
				// If hash is zero, use number-based request
				if hash == (common.Hash{}) {
					req66.GetBlockHeadersPacket.Origin.Number = req.Number
					req66.GetBlockHeadersPacket.Origin.Hash = common.Hash{}
				}
				bytes, err = rlp.EncodeToBytes(req66)
				if err != nil {
					cs.logger.Warn("Could not encode ETH/66+ header request", "err", err, "sentryIndex", i, "protocol", protocol.String())
					continue
				}
				messageId = proto_sentry.MessageId_GET_BLOCK_HEADERS_66

			default:
				continue
			}

			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: req.Number,
				Data: &proto_sentry.OutboundMessageData{
					Id:   messageId,
					Data: bytes,
				},
				MaxPeers: uint64(maxPeers),
			}

			sentPeers, err = cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
			if err != nil {
				// Only log errors, not "no peers" cases (those are logged by SendMessageByMinBlock)
				cs.logger.Warn("Could not send header request",
					"protocol", protocol.String(),
					"messageId", messageId.String(),
					"sentryIndex", i,
					"err", err)
				continue
			}

			// Check for empty peers - if we got peers, this protocol version works!
			// if sentPeers != nil && len(sentPeers.Peers) > 0 {
			// 	successfulProtocol = protocol
			// 	successfulMessageId = messageId
			// 	break // Found a working protocol version
			// }
		}

		// If no protocol version succeeded, try next sentry
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}

		// Now safe to access sentPeers.Peers[0]
		peerID := sentry.ConvertH512ToPeerID(sentPeers.Peers[0])

		// Get peer name for logging (only on Info level to reduce overhead)
		// peerName := cs.getPeerName(ctx, cs.sentries[i], sentPeers.Peers[0])

		// cs.logger.Info("Sent header request to peer",
		// 	"protocol", successfulProtocol.String(),
		// 	"messageId", successfulMessageId.String(),
		// 	"peer", fmt.Sprintf("%x", peerID[:8]),
		// 	"peerName", peerName,
		// 	"sentryIndex", i,
		// 	"number", req.Number,
		// 	"hash", req.Hash.Hex())
		return peerID, true
	}

	return [64]byte{}, false
}

func (cs *MultiClient) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(cs.sentries) > 1 {
		i = rand.Intn(len(cs.sentries) - 1) // nolint: gosec
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(cs.sentries)
		return i, i != to
	}
}

// sending list of penalties to all sentries
func (cs *MultiClient) Penalize(ctx context.Context, penalties []headerdownload.PenaltyItem) {
	for i := range penalties {
		outreq := proto_sentry.PenalizePeerRequest{
			PeerId:  gointerfaces.ConvertHashToH512(penalties[i].PeerID),
			Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
			if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
				continue
			}

			if _, err1 := cs.sentries[i].PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				cs.logger.Error("Could not send penalty", "err", err1)
			}
		}
	}
}
