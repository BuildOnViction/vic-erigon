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
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon-p2p/sentry"
	"github.com/erigontech/erigon/turbo/stages/bodydownload"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

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
	// fmt.Println("----> SendBodyRequest", req.BlockNums)

	// Try different ways to get protocol information
	var currentProtocol proto_sentry.Protocol
	var protocolDetected bool

	for _, sentry := range cs.sentries {
		if protocolMethod, ok := sentry.(interface{ Protocol() uint }); ok {
			protocolUint := protocolMethod.Protocol()
			// Convert uint to proto_sentry.Protocol
			switch protocolUint {
			case 63:
				currentProtocol = proto_sentry.Protocol_ETH63
				protocolDetected = true
				break
			default:
				currentProtocol = proto_sentry.Protocol_ETH67
				protocolDetected = true
				break
			}
			if protocolDetected {
				break
			}
		}
	}

	if !protocolDetected {
		for _, sentry := range cs.sentries {
			if protocolMethod, ok := sentry.(interface{ GetProtocol() proto_sentry.Protocol }); ok {
				currentProtocol = protocolMethod.GetProtocol()
				protocolDetected = true
				break
			}
		}
	}

	if !protocolDetected {
		if ctxProtocol, ok := ctx.Value("protocol").(proto_sentry.Protocol); ok {
			currentProtocol = ctxProtocol
			protocolDetected = true
		}
	}

	// Default to ETH/67 if no protocol detected
	if !protocolDetected {
		currentProtocol = proto_sentry.Protocol_ETH67
	}
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
		var bytes []byte
		var err error
		var messageId proto_sentry.MessageId

		// Use appropriate format based on detected protocol
		switch currentProtocol {
		case proto_sentry.Protocol_ETH63:
			bytes, err = rlp.EncodeToBytes(eth.GetBlockBodiesPacket(req.Hashes))
			messageId = proto_sentry.MessageId_GET_BLOCK_BODIES_63
			// fmt.Println("---> SendBodyRequest ETH/63 format:", bytes)
		default:
			// Default to ETH/66+ for other protocols
			bytes, err = rlp.EncodeToBytes(&eth.GetBlockBodiesPacket66{
				RequestId:            rand.Uint64(), // nolint: gosec
				GetBlockBodiesPacket: req.Hashes,
			})
			messageId = proto_sentry.MessageId_GET_BLOCK_BODIES_66
			// fmt.Println("---> SendBodyRequest ETH/66+ format (default):", bytes)
		}

		for i, b := range req.Hashes {
			fmt.Println("----> req body for hash:", req.BlockNums[i], b.String())
		}
		if err != nil {
			cs.logger.Error("Could not encode block bodies request", "err", err)
			return [64]byte{}, false
		}
		outreq := proto_sentry.SendMessageByMinBlockRequest{
			MinBlock: req.BlockNums[len(req.BlockNums)-1],
			Data: &proto_sentry.OutboundMessageData{
				Id:   messageId,
				Data: bytes,
			},
			MaxPeers: 1,
		}

		// fmt.Printf("----> SendBodyRequest Message Details:\n")
		// fmt.Printf("  Message ID: %s (value: %d)\n", messageId.String(), int32(messageId))
		// fmt.Printf("  Data Size: %d bytes\n", len(bytes))
		// fmt.Printf("  Block Count: %d\n", len(req.BlockNums))

		sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			cs.logger.Error("Could not send block bodies request", "err", err1)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		peerId := sentry.ConvertH512ToPeerID(sentPeers.Peers[0])
		fmt.Println("----> sent SendBodyRequest to peer:", peerId[:8], i)
		return sentry.ConvertH512ToPeerID(sentPeers.Peers[0]), true
	}
	// fmt.Println("----> sent SendBodyRequest to no peer")
	return [64]byte{}, false
}

func (cs *MultiClient) SendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) (peerID [64]byte, ok bool) {
	// Get protocol from context or use default
	// fmt.Println("----> SendHeaderRequest header:", req)

	// If hash is zero but we have a number, try to look up the hash from database
	hash := req.Hash
	if hash == (common.Hash{}) && req.Number > 0 {
		cs.logger.Debug("[SendHeaderRequest] Hash is zero, looking up from database",
			"number", req.Number)

		// Look up the hash from the database using the block number
		err := cs.db.View(ctx, func(tx kv.Tx) error {
			var ok bool
			var err error
			hash, ok, err = cs.blockReader.CanonicalHash(ctx, tx, req.Number)
			if err != nil {
				return err
			}
			if !ok || hash == (common.Hash{}) {
				// Hash not found in database, will use number-based request
				cs.logger.Debug("[SendHeaderRequest] Could not find canonical hash for block number",
					"number", req.Number,
					"note", "Request will be sent as number-based")
			} else {
				cs.logger.Info("[SendHeaderRequest] Successfully looked up hash",
					"number", req.Number,
					"hash", hash.Hex())
			}
			return nil
		})
		if err != nil {
			cs.logger.Warn("[SendHeaderRequest] Failed to look up hash for block number",
				"number", req.Number,
				"err", err,
				"note", "Request will be sent as number-based")
		}
	}

	cs.logger.Debug("[SendHeaderRequest] Starting request",
		"number", req.Number,
		"hash", hash.Hex(), // Use the looked-up hash
		"originalHash", req.Hash.Hex(),
		"length", req.Length,
		"sentryCount", len(cs.sentries),
		"hashWasZero", req.Hash == (common.Hash{}))

	// Try each sentry until we find one that can handle the request
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			cs.logger.Debug("[SendHeaderRequest] Sentry not ready", "sentryIndex", i)
			continue
		}

		// Detect protocol for THIS specific sentry
		var protocol proto_sentry.Protocol
		var protocolDetected bool

		// Method 1: Try Protocol() method from this sentry
		if protocolMethod, ok := cs.sentries[i].(interface{ Protocol() uint }); ok {
			protocolUint := protocolMethod.Protocol()
			switch protocolUint {
			case 63:
				protocol = proto_sentry.Protocol_ETH63
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Detected ETH/63 from sentry", "sentryIndex", i, "version", protocolUint)
			case 67:
				protocol = proto_sentry.Protocol_ETH67
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Detected ETH/67 from sentry", "sentryIndex", i, "version", protocolUint)
			case 68:
				protocol = proto_sentry.Protocol_ETH68
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Detected ETH/68 from sentry", "sentryIndex", i, "version", protocolUint)
			default:
				protocol = proto_sentry.Protocol_ETH67
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Using default ETH/67 for sentry", "sentryIndex", i, "version", protocolUint)
			}
		}

		// Method 2: Try GetProtocol() method
		if !protocolDetected {
			if protocolMethod, ok := cs.sentries[i].(interface{ GetProtocol() proto_sentry.Protocol }); ok {
				protocol = protocolMethod.GetProtocol()
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Detected protocol via GetProtocol", "sentryIndex", i, "protocol", protocol.String())
			}
		}

		// Method 3: Try context
		if !protocolDetected {
			if ctxProtocol, ok := ctx.Value("protocol").(proto_sentry.Protocol); ok {
				protocol = ctxProtocol
				protocolDetected = true
				cs.logger.Debug("[SendHeaderRequest] Using protocol from context", "sentryIndex", i, "protocol", protocol.String())
			}
		}

		// Default to ETH/67 if not detected
		if !protocolDetected {
			protocol = proto_sentry.Protocol_ETH67
			cs.logger.Debug("[SendHeaderRequest] Using default protocol", "sentryIndex", i, "protocol", protocol.String())
		}

		var (
			bytes     []byte
			err       error
			messageId proto_sentry.MessageId
			maxPeers  int32 = 5 // Default number of peers to try
		)

		// Create the request data
		requestID := rand.Uint64()

		// Use the appropriate message format based on THIS sentry's protocol
		switch protocol {
		case proto_sentry.Protocol_ETH63:
			// For ETH/63, use the legacy format without request ID
			req63 := &eth.GetBlockHeadersPacket{
				Origin: eth.HashOrNumber{
					Hash:   hash, // Use the looked-up hash
					Number: 0,    // Must be 0 when hash is provided (HashOrNumber encoding requirement)
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
			var err error
			bytes, err = rlp.EncodeToBytes(req63)
			if err != nil {
				cs.logger.Error("Could not encode ETH/63 header request", "err", err, "sentryIndex", i)
				continue
			}
			messageId = proto_sentry.MessageId_GET_BLOCK_HEADERS_63
			cs.logger.Debug("[SendHeaderRequest] Encoded ETH/63 request",
				"messageId", messageId.String(),
				"size", len(bytes),
				"number", req.Number,
				"hash", hash.Hex(), // Log the actual hash being used
				"sentryIndex", i,
				"usingHash", hash != (common.Hash{}))

		case proto_sentry.Protocol_ETH67, proto_sentry.Protocol_ETH68:
			// For ETH/66+, use format with request ID
			req66 := &eth.GetBlockHeadersPacket66{
				RequestId: requestID,
				GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
					Origin: eth.HashOrNumber{
						Hash:   hash, // Use the looked-up hash
						Number: 0,    // Must be 0 when hash is provided (HashOrNumber encoding requirement)
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
			var err error
			bytes, err = rlp.EncodeToBytes(req66)
			if err != nil {
				cs.logger.Error("Could not encode ETH/66+ header request", "err", err, "sentryIndex", i)
				continue
			}
			messageId = proto_sentry.MessageId_GET_BLOCK_HEADERS_66
			cs.logger.Debug("[SendHeaderRequest] Encoded ETH/66+ request",
				"messageId", messageId.String(),
				"requestID", requestID,
				"size", len(bytes),
				"number", req.Number,
				"hash", hash.Hex(), // Log the actual hash being used
				"sentryIndex", i,
				"protocol", protocol.String(),
				"usingHash", hash != (common.Hash{}))

		default:
			cs.logger.Error("Unsupported protocol", "protocol", protocol.String(), "sentryIndex", i)
			continue
		}

		// Log request details for debugging
		cs.logger.Debug("Sending header request",
			"protocol", protocol.String(),
			"messageId", messageId.String(),
			"number", req.Number,
			"hash", hash.Hex(), // Log the actual hash being used
			"sentryIndex", i)

		outreq := proto_sentry.SendMessageByMinBlockRequest{
			MinBlock: req.Number,
			Data: &proto_sentry.OutboundMessageData{
				Id:   messageId,
				Data: bytes,
			},
			MaxPeers: uint64(maxPeers),
		}

		sentPeers, err := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err != nil {
			cs.logger.Error("Could not send header request",
				"protocol", protocol.String(),
				"messageId", messageId.String(),
				"sentryIndex", i,
				"err", err,
				"note", "Sentry may not support this protocol version")
			continue
		}

		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			cs.logger.Debug("No peers available for header request",
				"protocol", protocol.String(),
				"messageId", messageId.String(),
				"sentryIndex", i)
			continue
		}

		peerID := sentry.ConvertH512ToPeerID(sentPeers.Peers[0])
		cs.logger.Info("Sent header request to peer",
			"protocol", protocol.String(),
			"messageId", messageId.String(),
			"peer", fmt.Sprintf("%x", peerID[:8]),
			"sentryIndex", i,
			"peerCount", len(sentPeers.Peers),
			"number", req.Number,
			"hash", req.Hash.Hex())
		return peerID, true
	}

	// cs.logger.Warn("Failed to send header request to any peer",
	// 	"sentryCount", len(cs.sentries),
	// 	"number", req.Number,
	// 	"hash", req.Hash.Hex())
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
