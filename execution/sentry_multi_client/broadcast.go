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
	"errors"
	"fmt"
	"math/big"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

func (cs *MultiClient) PropagateNewBlockHashes(ctx context.Context, announces []headerdownload.Announce) {
	// fmt.Println("[PropagateNewBlockHashes]::1")
	// Try different ways to get protocol information
	var currentProtocol proto_sentry.Protocol
	var protocolDetected bool

	// Method 1: Try Protocol() method (returns uint) from sentries
	for _, sentry := range cs.sentries {
		if protocolMethod, ok := sentry.(interface{ Protocol() uint }); ok {
			protocolUint := protocolMethod.Protocol()
			// Convert uint to proto_sentry.Protocol
			switch protocolUint {
			case 63:
				// fmt.Println("[7s62::PropagateNewBlockHashes] protocolUint 63")
				currentProtocol = proto_sentry.Protocol_ETH63
				protocolDetected = true
				break
			default:
				// fmt.Println("[7s62::PropagateNewBlockHashes] protocolUint 67")
				currentProtocol = proto_sentry.Protocol_ETH67
				protocolDetected = true
				break
			}
			if protocolDetected {
				break
			}
		}
	}

	// Method 2: Try GetProtocol() method (returns proto_sentry.Protocol) from sentries
	if !protocolDetected {
		// fmt.Println("[7s62::PropagateNewBlockHashes] protocolDetected false")
		for _, sentry := range cs.sentries {
			if protocolMethod, ok := sentry.(interface{ GetProtocol() proto_sentry.Protocol }); ok {
				// fmt.Println("[7s62::PropagateNewBlockHashes] protocolMethod GetProtocol")
				// fmt.Println("[7s62::PropagateNewBlockHashes] protocolMethod GetProtocol", protocolMethod.GetProtocol())
				currentProtocol = protocolMethod.GetProtocol()
				// fmt.Println("[7s62::PropagateNewBlockHashes] currentProtocol", currentProtocol)
				protocolDetected = true
				break
			}
		}
	}

	// Method 3: Check context for protocol
	if !protocolDetected {
		// fmt.Println("[7s62::PropagateNewBlockHashes] ctxProtocol", ctx.Value("protocol"))
		if ctxProtocol, ok := ctx.Value("protocol").(proto_sentry.Protocol); ok {
			// fmt.Println("[7s62::PropagateNewBlockHashes] ctxProtocol ok", ctxProtocol)
			currentProtocol = ctxProtocol
			protocolDetected = true
		}
	}

	// Default to ETH/67 if no protocol detected
	if !protocolDetected {
		// fmt.Println("[7s62::PropagateNewBlockHashes] default to ETH/67")
		currentProtocol = proto_sentry.Protocol_ETH67
	}

	// Determine if we should use ETH/63 format
	isETH63 := (currentProtocol == proto_sentry.Protocol_ETH63)

	typedRequest := make(eth.NewBlockHashesPacket, len(announces))
	for i := range announces {
		typedRequest[i].Hash = announces[i].Hash
		typedRequest[i].Number = announces[i].Number
	}

	data, err := rlp.EncodeToBytes(&typedRequest)
	if err != nil {
		log.Error("propagateNewBlockHashes", "err", err)
		return
	}

	// Handle ETH/63 vs ETH/66+ based on protocol version
	var req proto_sentry.OutboundMessageData
	if isETH63 {

		// ETH/63: No request ID, just raw data
		req = proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_NEW_BLOCK_HASHES_63,
			Data: data,
		}
	} else {
		// ETH/66+: With request ID
		req = proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_NEW_BLOCK_HASHES_66,
			Data: data,
		}
	}

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		_, err = sentry.SendMessageToAll(ctx, &req, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("propagateNewBlockHashes", "err", err)
		}
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (cs *MultiClient) BroadcastNewBlock(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {
	block, err := types.RawBlock{Header: header, Body: body}.AsBlock()
	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
		return
	}

	data, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: block,
		TD:    td,
	})
	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
		return
	}
	fmt.Println("[7s62::BroadcastNewBlock::11] data", data)
	// Detect protocol version
	var currentProtocol proto_sentry.Protocol
	var protocolDetected bool

	// Method 1: Try Protocol() method (returns uint) from sentries
	for _, sentry := range cs.sentries {
		if protocolMethod, ok := sentry.(interface{ Protocol() uint }); ok {
			protocolUint := protocolMethod.Protocol()
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

	// Method 2: Try GetProtocol() method (returns proto_sentry.Protocol) from sentries
	if !protocolDetected {
		for _, sentry := range cs.sentries {
			if protocolMethod, ok := sentry.(interface{ GetProtocol() proto_sentry.Protocol }); ok {
				currentProtocol = protocolMethod.GetProtocol()
				protocolDetected = true
				break
			}
		}
	}

	// Default to ETH/67 if no protocol detected
	if !protocolDetected {
		currentProtocol = proto_sentry.Protocol_ETH67
	}

	// Determine message ID based on protocol
	var messageId proto_sentry.MessageId
	if currentProtocol == proto_sentry.Protocol_ETH63 {
		messageId = proto_sentry.MessageId_NEW_BLOCK_63
	} else {
		messageId = proto_sentry.MessageId_NEW_BLOCK_66
	}

	req := proto_sentry.SendMessageToRandomPeersRequest{
		MaxPeers: uint64(cs.maxBlockBroadcastPeers(header)),
		Data: &proto_sentry.OutboundMessageData{
			Id:   messageId,
			Data: data,
		},
	}

	fmt.Printf("[7s62::BroadcastNewBlock] Protocol: %v (value: %d), MessageId: %s (value: %d)\n",
		currentProtocol.String(), int32(currentProtocol), messageId.String(), int32(messageId))

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		_, err = sentry.SendMessageToRandomPeers(ctx, &req, &grpc.EmptyCallOption{})
		if err != nil {
			if isPeerNotFoundErr(err) || networkTemporaryErr(err) {
				log.Debug("broadcastNewBlock", "err", err)
				continue
			}
			log.Error("broadcastNewBlock", "err", err)
		}
	}
}

func networkTemporaryErr(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, p2p.ErrShuttingDown)
}
func isPeerNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "peer not found")
}
