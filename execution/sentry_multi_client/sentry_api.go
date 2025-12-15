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
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		reqData := &eth.GetBlockHeadersPacket{
			Amount:  req.Length,
			Reverse: req.Reverse,
			Skip:    req.Skip,
			Origin:  eth.HashOrNumber{Hash: req.Hash},
		}

		if req.Hash == (common.Hash{}) {
			reqData.Origin.Number = req.Number
		}
		// log.Info(fmt.Sprintf("----> Sending header request {hash: %x,number: %d, height: %d, length: %d}", req.Hash, req.Number, req.Length, reqData.Origin.Number))

		bytes, err := rlp.EncodeToBytes(reqData)
		if err != nil {
			cs.logger.Error("Could not encode header request", "err", err)
			return [64]byte{}, false
		}
		minBlock := req.Number

		outreq := proto_sentry.SendMessageByMinBlockRequest{
			MinBlock: minBlock,
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_63,
				Data: bytes,
			},
			MaxPeers: 5,
		}
		sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		// fmt.Println("-> sent peers", sentPeers)
		if err1 != nil {
			cs.logger.Error("Could not send header request", "err", err1)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return sentry.ConvertH512ToPeerID(sentPeers.Peers[0]), true
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
