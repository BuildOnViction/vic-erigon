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

package sentry

import (
	"fmt"
	"io"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon-p2p/forkid"
	"github.com/erigontech/erigon-p2p/protocols/eth"
)

func readAndValidatePeerStatusMessage(
	rw p2p.MsgReadWriter,
	status *proto_sentry.StatusData,
	version uint,
	minVersion uint,
	logger log.Logger,
) (*eth.StatusPacket, *p2p.PeerError) {
	msg, err := rw.ReadMsg()
	if err != nil {
		return nil, p2p.NewPeerError(p2p.PeerErrorStatusReceive, p2p.DiscNetworkError, err, "readAndValidatePeerStatusMessage rw.ReadMsg error")
	}

	// Log that we received a message, regardless of validity
	logger.Info("-> Received message from peer",
		"msgCode", fmt.Sprintf("0x%02x", msg.Code),
		"msgSize", msg.Size,
		"expectedCode", fmt.Sprintf("0x%02x", eth.StatusMsg))

	reply, err := tryDecodeStatusMessage(&msg)
	msg.Discard()
	if err != nil {
		// Don't log error, just return it silently
		return nil, p2p.NewPeerError(p2p.PeerErrorStatusDecode, p2p.DiscProtocolError, err, "readAndValidatePeerStatusMessage tryDecodeStatusMessage error")
	}

	logger.Info("-> Received peer status message",
		"networkID", reply.NetworkID,
		"protocolVersion", reply.ProtocolVersion,
		"genesis", reply.Genesis.Hex(),
		"head", reply.Head.Hex(),
		"forkID", fmt.Sprintf("%x/%d", reply.ForkID.Hash, reply.ForkID.Next))

	err = checkPeerStatusCompatibility(reply, status, version, minVersion, logger)
	if err != nil {
		ourGenesisBytes := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
		ourGenesis := common.BytesToHash(ourGenesisBytes[:])
		logger.Warn("-> Status message rejected - incompatibility detected",
			"reason", err.Error(),
			"peerNetworkID", reply.NetworkID,
			"ourNetworkID", status.NetworkId,
			"peerGenesis", reply.Genesis.Hex(),
			"ourGenesis", ourGenesis.Hex(),
			"peerProtocolVersion", reply.ProtocolVersion,
			"ourMaxVersion", version,
			"ourMinVersion", minVersion,
			"peerForkID", fmt.Sprintf("%x/%d", reply.ForkID.Hash, reply.ForkID.Next))
		return nil, p2p.NewPeerError(p2p.PeerErrorStatusIncompatible, p2p.DiscUselessPeer, err, "readAndValidatePeerStatusMessage checkPeerStatusCompatibility error")
	}

	logger.Info("-> Status message validated successfully",
		"networkID", reply.NetworkID,
		"protocolVersion", reply.ProtocolVersion,
		"genesis", reply.Genesis.Hex())
	return reply, nil
}

func tryDecodeStatusMessage(msg *p2p.Msg) (*eth.StatusPacket, error) {
	if msg.Code != eth.StatusMsg {
		return nil, fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}

	if msg.Size > eth.ProtocolMaxMsgSize {
		return nil, fmt.Errorf("message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
	}

	// Read the message data into a buffer
	data := make([]byte, msg.Size)
	if _, err := io.ReadFull(msg.Payload, data); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// First try to decode as modern StatusPacket (ETH/64+)
	var reply eth.StatusPacket
	if err := rlp.DecodeBytes(data, &reply); err == nil {
		return &reply, nil
	}

	// If that fails, try to decode as legacy StatusPacket63 (ETH/63)
	var reply63 eth.StatusPacket63
	if err := rlp.DecodeBytes(data, &reply63); err != nil {
		return nil, fmt.Errorf("decode message as both modern and ETH/63 format failed: %w", err)
	}

	// Convert StatusPacket63 to StatusPacket with empty ForkID
	modernReply := &eth.StatusPacket{
		ProtocolVersion: reply63.ProtocolVersion,
		NetworkID:       reply63.NetworkID,
		TD:              reply63.TD,
		Head:            reply63.Head,
		Genesis:         reply63.Genesis,
		ForkID:          forkid.ID{}, // Empty ForkID for ETH/63
	}

	return modernReply, nil
}

func checkPeerStatusCompatibility(
	reply *eth.StatusPacket,
	status *proto_sentry.StatusData,
	version uint,
	minVersion uint,
	logger log.Logger,
) error {
	networkID := status.NetworkId
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)

	// Check Network ID
	if reply.NetworkID != networkID {
		// Special case: allow Network ID 1 when Chain ID is 1337 (Geth 1.9.9 behavior)
		if reply.NetworkID == 1 && networkID == 1337 {
			logger.Debug("-> Allowing Network ID 1 for Chain ID 1337 (Geth 1.9.9 compatibility)")
		} else {
			logger.Warn("-> Network ID mismatch",
				"peerNetworkID", reply.NetworkID,
				"ourNetworkID", networkID)
			return fmt.Errorf("network id does not match: theirs %d, ours %d", reply.NetworkID, networkID)
		}
	}

	// Check Protocol Version
	if uint(reply.ProtocolVersion) > version {
		logger.Warn("-> Protocol version too high",
			"peerVersion", reply.ProtocolVersion,
			"maxSupported", version)
		return fmt.Errorf("version is more than what this senty supports: theirs %d, max %d", reply.ProtocolVersion, version)
	}
	if uint(reply.ProtocolVersion) < minVersion {
		logger.Warn("-> Protocol version too low",
			"peerVersion", reply.ProtocolVersion,
			"minRequired", minVersion)
		return fmt.Errorf("version is less than allowed minimum: theirs %d, min %d", reply.ProtocolVersion, minVersion)
	}

	// Check Genesis Hash
	genesisHashCommon := common.BytesToHash(genesisHash[:])
	if reply.Genesis != genesisHashCommon {
		logger.Warn("-> Genesis hash mismatch",
			"peerGenesis", reply.Genesis.Hex(),
			"ourGenesis", genesisHashCommon.Hex())
		return fmt.Errorf("genesis hash does not match: theirs %x, ours %x", reply.Genesis, genesisHash)
	}

	// Check ForkID
	// Skip ForkID validation for ETH/63 (empty ForkID)
	if reply.ForkID.Hash != [4]byte{} || reply.ForkID.Next != 0 {
		forkFilter := forkid.NewFilterFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime)
		if err := forkFilter(reply.ForkID); err != nil {
			logger.Warn("-> ForkID mismatch",
				"peerForkID", fmt.Sprintf("%x/%d", reply.ForkID.Hash, reply.ForkID.Next),
				"error", err.Error())
			return err
		}
		logger.Debug("-> ForkID validated successfully",
			"forkID", fmt.Sprintf("%x/%d", reply.ForkID.Hash, reply.ForkID.Next))
	} else {
		logger.Debug("-> Skipping ForkID validation for ETH/63 peer")
	}

	return nil
}
