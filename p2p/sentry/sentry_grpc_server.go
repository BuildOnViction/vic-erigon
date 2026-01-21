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
	"bytes"
	"container/heap"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/debug"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/eth63"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon-p2p/dnsdisc"
	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon-p2p/forkid"
	"github.com/erigontech/erigon-p2p/protocols/eth"
)

const (
	// handshakeTimeout is the maximum allowed time for the `eth` handshake to
	// complete before dropping the connection.= as malicious.
	handshakeTimeout  = 5 * time.Second
	maxPermitsPerPeer = 4 // How many outstanding requests per peer we may have
)

// PeerInfo collects various extra bits of information about the peer,
// for example deadlines that is used for regulating requests sent to the peer
type PeerInfo struct {
	peer          *p2p.Peer
	lock          sync.RWMutex
	deadlines     []time.Time // Request deadlines
	latestDealine time.Time
	height        uint64
	rw            p2p.MsgReadWriter
	protocol      uint

	ctx       context.Context
	ctxCancel context.CancelFunc

	// this channel is closed on Remove()
	removed      chan struct{}
	removeReason *p2p.PeerError
	removeOnce   sync.Once

	// each peer has own worker (goroutine) - all funcs from this queue will execute on this worker
	// if this queue is full (means peer is slow) - old messages will be dropped
	// channel closed on peer remove
	tasks chan func()
}

type PeerRef struct {
	pi     *PeerInfo
	height uint64
}

// PeersByMinBlock is the priority queue of peers. Used to select certain number of peers considered to be "best available"
type PeersByMinBlock []PeerRef

// Len (part of heap.Interface) returns the current size of the best peers queue
func (bp PeersByMinBlock) Len() int {
	return len(bp)
}

// Less (part of heap.Interface) compares two peers
func (bp PeersByMinBlock) Less(i, j int) bool {
	return bp[i].height < bp[j].height
}

// Swap (part of heap.Interface) moves two peers in the queue into each other's places.
func (bp PeersByMinBlock) Swap(i, j int) {
	bp[i], bp[j] = bp[j], bp[i]
}

// Push (part of heap.Interface) places a new peer onto the end of queue.
func (bp *PeersByMinBlock) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	p := x.(PeerRef)
	*bp = append(*bp, p)
}

// Pop (part of heap.Interface) removes the first peer from the queue
func (bp *PeersByMinBlock) Pop() interface{} {
	old := *bp
	n := len(old)
	x := old[n-1]
	old[n-1] = PeerRef{} // avoid memory leak
	*bp = old[0 : n-1]
	return x
}

func NewPeerInfo(peer *p2p.Peer, rw p2p.MsgReadWriter) *PeerInfo {
	ctx, cancel := context.WithCancel(context.Background())

	p := &PeerInfo{
		peer:      peer,
		rw:        rw,
		removed:   make(chan struct{}),
		tasks:     make(chan func(), 32),
		ctx:       ctx,
		ctxCancel: cancel,
	}

	p.lock.RLock()
	t := p.tasks
	p.lock.RUnlock()

	go func() { // each peer has own worker, then slow
		for f := range t {
			f()
		}
	}()
	return p
}

func (pi *PeerInfo) Close() {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	if pi.tasks != nil {
		close(pi.tasks)
		// Setting this to nil because other functions detect the closure of the channel by checking pi.tasks == nil
		pi.tasks = nil
	}
}

func (pi *PeerInfo) ID() [64]byte {
	return pi.peer.Pubkey()
}

// AddDeadline adds given deadline to the list of deadlines
// Deadlines must be added in the chronological order for the function
// ClearDeadlines to work correctly (it uses binary search)
func (pi *PeerInfo) AddDeadline(deadline time.Time) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.deadlines = append(pi.deadlines, deadline)
	pi.latestDealine = deadline
}

func (pi *PeerInfo) Height() uint64 {
	return atomic.LoadUint64(&pi.height)
}

// SetIncreasedHeight atomically updates PeerInfo.height only if newHeight is higher
func (pi *PeerInfo) SetIncreasedHeight(newHeight uint64) {
	for {
		oldHeight := atomic.LoadUint64(&pi.height)
		if oldHeight >= newHeight || atomic.CompareAndSwapUint64(&pi.height, oldHeight, newHeight) {
			break
		}
	}
}

// ClearDeadlines goes through the deadlines of
// given peers and removes the ones that have passed
// Optionally, it also clears one extra deadline - this is used when response is received
// It returns the number of deadlines left
func (pi *PeerInfo) ClearDeadlines(now time.Time, givePermit bool) int {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	// Look for the first deadline which is not passed yet
	firstNotPassed := sort.Search(len(pi.deadlines), func(i int) bool {
		return pi.deadlines[i].After(now)
	})
	cutOff := firstNotPassed
	if cutOff < len(pi.deadlines) && givePermit {
		cutOff++
	}
	pi.deadlines = pi.deadlines[cutOff:]
	return len(pi.deadlines)
}

func (pi *PeerInfo) LatestDeadline() time.Time {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.latestDealine
}

func (pi *PeerInfo) Remove(reason *p2p.PeerError) {
	pi.removeOnce.Do(func() {
		pi.removeReason = reason
		close(pi.removed)
		pi.ctxCancel()
		pi.peer.Disconnect(reason)
	})
}

func (pi *PeerInfo) Async(f func(), logger log.Logger) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	if pi.tasks == nil {
		// Too late, the task channel has been closed
		return
	}
	select {
	case <-pi.removed: // noop if peer removed
	case <-pi.ctx.Done():
		if pi.tasks != nil {
			close(pi.tasks)
			// Setting this to nil because other functions detect the closure of the channel by checking pi.tasks == nil
			pi.tasks = nil
		}
	case pi.tasks <- f:
		if len(pi.tasks) == cap(pi.tasks) { // if channel full - discard old messages
			for i := 0; i < cap(pi.tasks)/2; i++ {
				select {
				case <-pi.tasks:
				default:
				}
			}
			logger.Trace("[sentry] slow peer or too many requests, dropping its old requests", "name", pi.peer.Name())
		}
	}
}

func (pi *PeerInfo) RemoveReason() *p2p.PeerError {
	select {
	case <-pi.removed:
		return pi.removeReason
	default:
		return nil
	}
}

// ConvertH512ToPeerID() ensures the return type is [64]byte
// so that short variable declarations will still be formatted as hex in logs
func ConvertH512ToPeerID(h512 *proto_types.H512) [64]byte {
	return gointerfaces.ConvertH512ToHash(h512)
}

func makeP2PServer(
	p2pConfig p2p.Config,
	genesisHash common.Hash,
	protocols []p2p.Protocol,
) (*p2p.Server, error) {
	if len(p2pConfig.BootstrapNodes) == 0 {
		urls := p2pConfig.LookupBootnodeURLs(genesisHash)
		bootstrapNodes, err := enode.ParseNodesFromURLs(urls)
		if err != nil {
			return nil, fmt.Errorf("bad bootnodes option: %w", err)
		}
		p2pConfig.BootstrapNodes = bootstrapNodes
		p2pConfig.BootstrapNodesV5 = bootstrapNodes
	}
	p2pConfig.Protocols = protocols
	return &p2p.Server{Config: p2pConfig}, nil
}

func handShake(
	ctx context.Context,
	status *proto_sentry.StatusData,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	logger log.Logger,
) (*common.Hash, *p2p.PeerError) {
	// Send out own handshake in a new thread
	errChan := make(chan *p2p.PeerError, 2)
	resultChan := make(chan *eth.StatusPacket, 1)

	ourTD := gointerfaces.ConvertH256ToUint256Int(status.TotalDifficulty)
	// Convert proto status data into the one required by devp2p
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)

	go func() {
		defer debug.LogPanic()

		if version == 63 {
			// Send ETH/63 format (no ForkID)
			status63 := &eth.StatusPacket63{
				ProtocolVersion: uint32(version),
				NetworkID:       status.NetworkId,
				TD:              ourTD.ToBig(),
				Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
				Genesis:         genesisHash,
			}
			logger.Info("-> Sending ETH/63 status message",
				"networkID", status63.NetworkID,
				"protocolVersion", status63.ProtocolVersion,
				"genesis", status63.Genesis.Hex(),
				"head", status63.Head.Hex(),
				"td", ourTD.ToBig().String())
			err := p2p.Send(rw, eth.StatusMsg, status63)
			if err == nil {
				logger.Debug("-> Status message sent successfully (ETH/63)")
				errChan <- nil
			} else {
				logger.Warn("-> Failed to send status message", "err", err)
				errChan <- p2p.NewPeerError(p2p.PeerErrorStatusSend, p2p.DiscNetworkError, err, "sentry.handShake failed to send eth Status")
			}
		} else {
			// Send ETH/64+ format (with ForkID)
			forkID := forkid.NewIDFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime)
			status64 := &eth.StatusPacket{
				ProtocolVersion: uint32(version),
				NetworkID:       status.NetworkId,
				TD:              ourTD.ToBig(),
				Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
				Genesis:         genesisHash,
				ForkID:          forkID,
			}
			logger.Info("-> Sending ETH/64+ status message",
				"networkID", status64.NetworkID,
				"protocolVersion", status64.ProtocolVersion,
				"genesis", status64.Genesis.Hex(),
				"head", status64.Head.Hex(),
				"forkID", fmt.Sprintf("%x/%d", forkID.Hash, forkID.Next),
				"td", ourTD.ToBig().String())
			err := p2p.Send(rw, eth.StatusMsg, status64)
			if err == nil {
				logger.Debug("-> Status message sent successfully (ETH/64+)")
				errChan <- nil
			} else {
				logger.Warn("-> Failed to send status message", "err", err)
				errChan <- p2p.NewPeerError(p2p.PeerErrorStatusSend, p2p.DiscNetworkError, err, "sentry.handShake failed to send eth Status")
			}
		}
	}()

	go func() {
		defer debug.LogPanic()
		logger.Debug("-> Waiting for peer status message")
		peerStatus, err := readAndValidatePeerStatusMessage(rw, status, version, minVersion, logger)

		if err == nil {
			logger.Info("-> Received and validated peer status message",
				"networkID", peerStatus.NetworkID,
				"protocolVersion", peerStatus.ProtocolVersion,
				"genesis", peerStatus.Genesis.Hex(),
				"head", peerStatus.Head.Hex(),
				"forkID", fmt.Sprintf("%x/%d", peerStatus.ForkID.Hash, peerStatus.ForkID.Next))
			resultChan <- peerStatus
			errChan <- nil
		} else {
			logger.Debug("-> Failed to receive/validate peer status", "err", err)
			errChan <- err
		}
	}()

	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				fmt.Printf("-> Handshake failed: %v\n", err)
				return nil, err
			}
		case <-timeout.C:
			logger.Warn("-> Handshake timeout")
			return nil, p2p.NewPeerError(p2p.PeerErrorStatusHandshakeTimeout, p2p.DiscReadTimeout, nil, "sentry.handShake timeout")
		case <-ctx.Done():
			logger.Debug("-> Handshake cancelled")
			return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.handShake ctx.Done")
		}
	}

	peerStatus := <-resultChan
	logger.Info("-> Handshake successful",
		"peerHead", peerStatus.Head.Hex(),
		"networkID", peerStatus.NetworkID,
		"protocolVersion", peerStatus.ProtocolVersion)
	return &peerStatus.Head, nil
}

func runPeer(
	ctx context.Context,
	peerID [64]byte,
	cap p2p.Cap,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId proto_sentry.MessageId) bool,
	logger log.Logger,
) *p2p.PeerError {
	protocol := cap.Version
	printTime := time.Now().Add(time.Minute)
	peerPrinted := false

	fmt.Printf("-> Starting message loop for peer %x, protocol=%d\n", peerID[:8], protocol)

	defer func() {
		select { // don't print logs if we stopping
		case <-ctx.Done():
			return
		default:
		}
		if peerPrinted {
			logger.Trace("Peer disconnected", "id", peerID, "name", peerInfo.peer.Fullname())
		}
		fmt.Printf("-> Message loop ended for peer %x\n", peerID[:8])
	}()

	for {
		if !peerPrinted {
			if time.Now().After(printTime) {
				logger.Trace("Peer stable", "id", peerID, "name", peerInfo.peer.Fullname())
				peerPrinted = true
			}
		}
		if err := common.Stopped(ctx.Done()); err != nil {
			fmt.Printf("-> Context stopped for peer %x: %v\n", peerID[:8], err)
			return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.runPeer: context stopped")
		}
		if err := peerInfo.RemoveReason(); err != nil {
			// fmt.Printf("-> Peer removed for peer %x: %v\n", peerID[:8], err)
			return err
		}

		msg, err := rw.ReadMsg()
		if err != nil {
			// fmt.Printf("-> ReadMsg error for peer %x: %v\n", peerID[:8], err)
			return p2p.NewPeerError(p2p.PeerErrorMessageReceive, p2p.DiscNetworkError, err, "sentry.runPeer: ReadMsg error")
		}

		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return p2p.NewPeerError(p2p.PeerErrorMessageSizeLimit, p2p.DiscSubprotocolError, nil, fmt.Sprintf("sentry.runPeer: message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize))
		}

		givePermit := false

		msgID := eth.ToProto[protocol][msg.Code]
		logger.Debug("Received message from peer",
			"peerID", hex.EncodeToString(peerID[:])[:20],
			"msgCode", msg.Code,
			"msgSize", msg.Size,
			"msgType", msgID.String())

		switch msg.Code {
		case eth.StatusMsg:
			// fmt.Printf("-> Received STATUS message from peer %x\n", peerID[:8])
			msg.Discard()
			return p2p.NewPeerError(p2p.PeerErrorStatusUnexpected, p2p.DiscSubprotocolError, nil, "sentry.runPeer: unexpected status message")
		case eth.GetBlockHeadersMsg:
			fmt.Printf("-> Received GET_BLOCK_HEADERS message from peer %x\n", peerID[:8])
			msgID := eth.ToProto[protocol][msg.Code]
			fmt.Printf("-> GET_BLOCK_HEADERS mapped to msgID=%s (protocol=%d)\n", msgID.String(), protocol)
			hasSubs := hasSubscribers(msgID)
			fmt.Printf("-> hasSubscribers(%s)=%v, protocol==63=%v\n", msgID.String(), hasSubs, protocol == 63)
			if !((protocol == 63) || hasSubs) {
				fmt.Printf("-> DROPPING GET_BLOCK_HEADERS: no subscribers and protocol!=63\n")
				continue
			}
			b := make([]byte, msg.Size)
			fmt.Println("7s62:GetBlockHeadersMsg", b)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			fmt.Println("7s62:GetBlockHeadersMsg::2", b)
			// Decode GET_BLOCK_HEADERS request (not response)
			fmt.Println("-> Received GET_BLOCK_HEADERS message from peer::startDecode")
			var req eth.GetBlockHeadersPacket
			if err := rlp.DecodeBytes(b, &req); err == nil {
				originStr := "hash"
				originVal := req.Origin.Hash.Hex()
				if req.Origin.Hash == (common.Hash{}) {
					originStr = "number"
					originVal = fmt.Sprintf("%d", req.Origin.Number)
				}
				fmt.Printf("-> Decoded GET_BLOCK_HEADERS: origin=%s(%s) amount=%d skip=%d reverse=%v\n",
					originStr, originVal, req.Amount, req.Skip, req.Reverse)
			} else {
				fmt.Printf("-> GET_BLOCK_HEADERS decode error: %v\n", err)
			}
			fmt.Printf("-> Calling send() for GET_BLOCK_HEADERS (msgID=%s, bytes=%d)\n", msgID.String(), len(b))
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockHeadersMsg:
			// fmt.Printf("-> Received BLOCK_HEADERS message from peer %x\n", peerID[:8])
			// For ETH/63, remap to standard message ID so client understands
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH63][msg.Code]
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			shouldForward := (protocol == 63) || hasSubscribers(msgID)
			if !shouldForward {
				fmt.Printf("-> No subscribers for %s, dropping message (peer=%x)\n", msgID.String(), peerID[:8])
				continue
			}
			// Read bytes first
			b := make([]byte, msg.Size)
			_, err := io.ReadFull(msg.Payload, b)
			if err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
				continue
			}

			// Decode and log BLOCK_HEADERS message
			if protocol == 63 {
				// ETH/63: BlockHeadersPacket (no request ID)
				// var headers eth.BlockHeadersPacket
				// if err := rlp.DecodeBytes(b, &headers); err == nil {
				// 	fmt.Printf("--> Decoded BLOCK_HEADERS (ETH/63) count=%d bytes=%d\n", len(headers), len(b))
				// 	// Log first 3 headers
				// 	for i := 0; i < len(headers) && i < 3; i++ {
				// 		h := headers[i]
				// 		fmt.Printf("   [%d] num=%d hash=%x parent=%x time=%d gasLimit=%d gasUsed=%d miner=%x difficulty=%d\n",
				// 			i, h.Number.Uint64(), h.Hash(), h.ParentHash, h.Time, h.GasLimit, h.GasUsed, h.Coinbase, h.Difficulty.Int64())
				// 	}
				// } else {
				// 	fmt.Printf("--> BLOCK_HEADERS (ETH/63) decode error: %v\n", err)
				// }
			} else {
				// ETH/66+: BlockHeadersPacket66 (with request ID)
				var headers eth.BlockHeadersPacket66
				if err := rlp.DecodeBytes(b, &headers); err == nil {
					fmt.Printf("--> Decoded BLOCK_HEADERS (ETH/%d) requestID=%d count=%d bytes=%d\n",
						protocol, headers.RequestId, len(headers.BlockHeadersPacket), len(b))
					// Log first 3 headers
					for i := 0; i < len(headers.BlockHeadersPacket) && i < 3; i++ {
						h := headers.BlockHeadersPacket[i]
						fmt.Printf("   [%d] num=%d hash=%x parent=%x time=%d gasLimit=%d gasUsed=%d miner=%x difficulty=%d\n",
							i, h.Number.Uint64(), h.Hash(), h.ParentHash, h.Time, h.GasLimit, h.GasUsed, h.Coinbase, h.Difficulty.Int64())
					}
				} else {
					fmt.Printf("--> BLOCK_HEADERS (ETH/%d) decode error: %v\n", protocol, err)
				}
			}

			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetBlockBodiesMsg:
			fmt.Printf("-> Received GET_BLOCK_BODIES message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			if protocol == 63 && !hasSubscribers(msgID) {
				fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockBodiesMsg:
			fmt.Printf("-> Received BLOCK_BODIES message from peer %x\n", peerID[:8])
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
				continue
			}

			// Decode and log BLOCK_BODIES message
			if protocol == 63 {
				// ETH/63: BlockRawBodiesPacket (no request ID)
				var bodies eth.BlockRawBodiesPacket
				if err := rlp.DecodeBytes(b, &bodies); err == nil {
					txs, uncles, withdrawals := bodies.Unpack()
					totalTxs := 0
					for _, blockTxs := range txs {
						totalTxs += len(blockTxs)
					}
					totalUncles := 0
					for _, blockUncles := range uncles {
						totalUncles += len(blockUncles)
					}
					fmt.Printf("--> Decoded BLOCK_BODIES (ETH/63) count=%d total_txs=%d total_uncles=%d bytes=%d\n",
						len(bodies), totalTxs, totalUncles, len(b))
					// Log first 3 blocks
					for i := 0; i < len(bodies) && i < 3; i++ {
						fmt.Printf("   [%d] txs=%d uncles=%d withdrawals=%d\n",
							i, len(txs[i]), len(uncles[i]), len(withdrawals[i]))
					}
				} else {
					fmt.Printf("--> BLOCK_BODIES (ETH/63) decode error: %v\n", err)
				}
			} else {
				// ETH/66+: BlockRawBodiesPacket66 (with request ID)
				var bodies eth.BlockRawBodiesPacket66
				if err := rlp.DecodeBytes(b, &bodies); err == nil {
					txs, uncles, withdrawals := bodies.BlockRawBodiesPacket.Unpack()
					totalTxs := 0
					for _, blockTxs := range txs {
						totalTxs += len(blockTxs)
					}
					totalUncles := 0
					for _, blockUncles := range uncles {
						totalUncles += len(blockUncles)
					}
					fmt.Printf("--> Decoded BLOCK_BODIES (ETH/%d) requestID=%d count=%d total_txs=%d total_uncles=%d bytes=%d\n",
						protocol, bodies.RequestId, len(bodies.BlockRawBodiesPacket), totalTxs, totalUncles, len(b))
					// Log first 3 blocks
					for i := 0; i < len(bodies.BlockRawBodiesPacket) && i < 3; i++ {
						fmt.Printf("   [%d] txs=%d uncles=%d withdrawals=%d\n",
							i, len(txs[i]), len(uncles[i]), len(withdrawals[i]))
					}
				} else {
					fmt.Printf("--> BLOCK_BODIES (ETH/%d) decode error: %v\n", protocol, err)
				}
			}

			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetReceiptsMsg:
			// fmt.Printf("-> Received GET_RECEIPTS message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			if protocol == 63 && !hasSubscribers(msgID) {
				// fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.ReceiptsMsg:
			// fmt.Printf("-> Received RECEIPTS message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			if protocol == 63 && !hasSubscribers(msgID) {
				// fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewBlockHashesMsg:
			// fmt.Printf("-> Received NEW_BLOCK_HASHES message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			if protocol == 63 && !hasSubscribers(msgID) {
				// fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewBlockMsg:
			// fmt.Printf("-> Received NEW_BLOCK message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			} else {
				var nb eth.NewBlockPacket
				if err := rlp.DecodeBytes(b, &nb); err == nil && nb.Block != nil {
					// h := nb.Block.Header()
					fmt.Printf("-> Decoded NEW_BLOCK num=%d hash=%x parent=%x td=%s txs=%d uncles=%d\n",
						nb.Block.NumberU64(), nb.Block.Hash(), nb.Block.ParentHash(),
						nb.TD.String(), len(nb.Block.Transactions()), len(nb.Block.Uncles()))
					maxTx := 5
					txs := nb.Block.Transactions()
					if len(txs) < maxTx {
						maxTx = len(txs)
					}
					for i := 0; i < maxTx; i++ {
						fmt.Printf("   tx[%d]=%x\n", i, txs[i].Hash())
					}
					// fmt.Printf("   miner=%x time=%d gasLimit=%d gasUsed=%d txRoot=%x receiptsRoot=%x stateRoot=%x\n",
					// h.Coinbase, h.Time, h.GasLimit, h.GasUsed, h.TxHash, h.ReceiptHash, h.Root)
				} else if err != nil {
					// fmt.Printf("-> NEW_BLOCK decode error: %v\n", err)
				}
			}
			if protocol == 63 && !hasSubscribers(msgID) {
				// fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewPooledTransactionHashesMsg:
			// fmt.Printf("-> Received NEW_POOLED_TRANSACTION_HASHES message from peer %x\n", peerID[:8])
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			logTransactionDetails(msg.Code, peerID[:], b, logger)

			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetPooledTransactionsMsg:
			// fmt.Printf("-> Received GET_POOLED_TRANSACTIONS message from peer %x\n", peerID[:8])
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			logTransactionDetails(msg.Code, peerID[:], b, logger)

			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.TransactionsMsg:
			// fmt.Printf("-> Received TRANSACTIONS message from peer %x\n", peerID[:8])
			var msgID proto_sentry.MessageId
			if protocol == 63 {
				msgID = eth.ToProto[direct.ETH67][msg.Code] // Use ETH/67 mapping for compatibility
			} else {
				msgID = eth.ToProto[protocol][msg.Code]
			}
			if !((protocol == 63) || hasSubscribers(msgID)) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}

			// Decode and log transaction details based on protocol version
			// fmt.Printf("-> Decoding transactions from peer %x, data size=%d bytes, protocol=%d\n", peerID[:8], len(b), protocol)

			if protocol == 63 {
				// Use ETH/63 transaction format
				var txs63 eth63.ETH63Transactions
				if err := rlp.DecodeBytes(b, &txs63); err == nil {
					fmt.Printf("-> Successfully decoded %d ETH/63 transactions\n", len(txs63))
					for i, tx := range txs63 {
						if i >= 3 { // Limit to first 3 transactions
							fmt.Printf("-> ... and %d more transactions\n", len(txs63)-3)
							break
						}
						fmt.Printf("-> ETH63 TX[%d]: hash=%x\n", i, tx.Hash())
						if tx.To() != nil {
							fmt.Printf("-> ETH63 TX[%d]: to=%x\n", i, *tx.To())
						} else {
							fmt.Printf("-> ETH63 TX[%d]: to=<contract creation>\n", i)
						}
						// Get sender using ETH/63 signer - use FrontierSigner for simplicity
						signer := eth63.HomesteadSigner{}
						if sender, err := eth63.Sender(signer, tx); err == nil {
							fmt.Printf("-> ETH63 TX[%d]: from=%x\n", i, sender)
						} else {
							fmt.Printf("-> ETH63 TX[%d]: from=<error: %v>\n", i, err)
						}
						fmt.Printf("-> ETH63 TX[%d]: value=%s gas=%d gasPrice=%s\n", i, tx.Value().String(), tx.Gas(), tx.GasPrice().String())
						fmt.Printf("-> ETH63 TX[%d]: nonce=%d dataLen=%d\n", i, tx.Nonce(), len(tx.Data()))
						if len(tx.Data()) > 0 {
							fmt.Printf("-> ETH63 TX[%d]: data=%x\n", i, tx.Data()[:min(64, len(tx.Data()))])
						}
						v, r, s := tx.RawSignatureValues()
						fmt.Printf("-> ETH63 TX[%d]: v=%s r=%s s=%s\n", i, v.String(), r.String(), s.String())
					}
				} else {
					fmt.Printf("-> Failed to decode ETH/63 transactions: %v\n", err)
					fmt.Printf("-> Raw data preview: %x\n", b[:min(100, len(b))])
				}
			} else {
				// Use modern transaction format
				fmt.Printf("-> Not ETH/63, Received modern transactions\n")
			}

			logTransactionDetails(msg.Code, peerID[:], b, logger)
			if protocol == 63 && !hasSubscribers(msgID) {
				// fmt.Printf("-> ETH/63 override: forwarding %s without subscribers (peer=%x)\n", msgID.String(), peerID[:8])
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.PooledTransactionsMsg:
			fmt.Printf("-> Received POOLED_TRANSACTIONS message from peer %x\n", peerID[:8])
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			logTransactionDetails(msg.Code, peerID[:], b, logger)

			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case 11:
			fmt.Printf("-> Received unknown message code 11 from peer %x\n", peerID[:8])
			// Ignore
			// TODO: Investigate why BSC peers for eth/67 send these messages
		default:
			logger.Error(fmt.Sprintf("[p2p] Unknown message code: %d, peerID=%x", msg.Code, peerID))
		}

		msgType := eth.ToProto[protocol][msg.Code]
		msgCap := cap.String()
		trackPeerStatistics(peerInfo.peer.Fullname(), peerInfo.peer.ID().String(), true, msgType.String(), msgCap, int(msg.Size))

		msg.Discard()
		peerInfo.ClearDeadlines(time.Now(), givePermit)
	}
}

// Add this function near the top of the file
func logTransactionDetails(msgCode uint64, peerID []byte, data []byte, logger log.Logger) {
	switch msgCode {
	case eth.NewPooledTransactionHashesMsg:
		logger.Info(fmt.Sprintf("[P2P] 📨 Node 2 received transaction hashes from peer %x", peerID))
		logger.Info(fmt.Sprintf("[P2P] 🏷️ Transaction hashes count: %d", len(data)/32))

		// Log individual hashes
		for i := 0; i < len(data); i += 32 {
			if i+32 <= len(data) {
				hash := data[i : i+32]
				logger.Info(fmt.Sprintf("[P2P] �� Hash %d: %x", i/32+1, hash))
			}
		}

	case eth.GetPooledTransactionsMsg:
		logger.Info(fmt.Sprintf("[P2P] 🔍 Node 2 received transaction request from peer %x", peerID))
		logger.Info(fmt.Sprintf("[P2P] �� Request data size: %d bytes", len(data)))

		// Try to parse the request to show what hashes are being requested
		if len(data) > 0 {
			logger.Info(fmt.Sprintf("[P2P] 🔍 Requesting transaction hashes: %x", data))
		}

	case eth.TransactionsMsg:
		logger.Info(fmt.Sprintf("[P2P] 💰 Node 2 received actual transactions from peer %x", peerID))
		logger.Info(fmt.Sprintf("[P2P] 📦 Transaction data size: %d bytes", len(data)))

		// Try to extract transaction count
		if len(data) > 0 {
			logger.Info(fmt.Sprintf("[P2P] 🔢 Estimated transactions in message: %d", len(data)/100)) // Rough estimate
		}

	case eth.PooledTransactionsMsg:
		logger.Info(fmt.Sprintf("[P2P] 📋 Node 2 received pooled transactions from peer %x", peerID))
		logger.Info(fmt.Sprintf("[P2P] 📊 Pooled transactions size: %d bytes", len(data)))

		// Try to parse the response to show transaction count
		if len(data) > 0 {
			logger.Info(fmt.Sprintf("[P2P] 🔢 Pooled transactions response: %x", data[:min(100, len(data))]))
		}
	}
}

func trackPeerStatistics(peerName string, peerID string, inbound bool, msgType string, msgCap string, bytes int) {
	isDiagEnabled := diagnostics.TypeOf(diagnostics.PeerStatisticMsgUpdate{}).Enabled()
	if isDiagEnabled {
		stats := diagnostics.PeerStatisticMsgUpdate{
			PeerName: peerName,
			PeerID:   peerID,
			Inbound:  inbound,
			MsgType:  msgType,
			MsgCap:   msgCap,
			Bytes:    bytes,
			PeerType: "Sentry",
		}

		diagnostics.Send(stats)
	}
}

func grpcSentryServer(ctx context.Context, sentryAddr string, ss *GrpcServer, healthCheck bool) (*grpc.Server, error) {
	// STARTING GRPC SERVER
	ss.logger.Info("Starting Sentry gRPC server", "on", sentryAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Sentry gRPC received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Sentry P2P listener: %w, addr=%s", err, sentryAddr)
	}
	grpcServer := grpcutil.NewServer(100, nil)
	proto_sentry.RegisterSentryServer(grpcServer, ss)
	var healthServer *health.Server
	if healthCheck {
		healthServer = health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	}

	go func() {
		if healthCheck {
			defer healthServer.Shutdown()
		}
		if err1 := grpcServer.Serve(lis); err1 != nil {
			ss.logger.Error("Sentry gRPC server fail", "err", err1)
		}
	}()
	return grpcServer, nil
}

func NewGrpcServer(ctx context.Context, dialCandidates func() enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint, logger log.Logger) *GrpcServer {
	ss := &GrpcServer{
		ctx:          ctx,
		p2p:          cfg,
		peersStreams: NewPeersStreams(),
		logger:       logger,
	}

	var disc enode.Iterator
	if dialCandidates != nil {
		disc = dialCandidates()
	} else {
		disc, _ = setupDiscovery(ss.p2p.DiscoveryDNS)
	}

	ss.Protocols = append(ss.Protocols, p2p.Protocol{
		Name:           eth.ProtocolName,
		Version:        protocol,
		Length:         17,
		DialCandidates: disc,
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
			peerID := peer.Pubkey()
			printablePeerID := hex.EncodeToString(peerID[:])[:20]
			if ss.getPeer(peerID) != nil {
				return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscAlreadyConnected, nil, "peer already has connection")
			}
			logger.Trace("[p2p] start with peer", "peerId", printablePeerID)

			peerInfo := NewPeerInfo(peer, rw)
			peerInfo.protocol = protocol
			defer peerInfo.Close()

			defer ss.GoodPeers.Delete(peerID)

			status := ss.GetStatus()

			if status == nil {
				return p2p.NewPeerError(p2p.PeerErrorLocalStatusNeeded, p2p.DiscProtocolError, nil, "could not get status message from core")
			}

			// Before the handshake
			logger.Info("--> Starting handshake with peer", "peerID", printablePeerID, "name", peer.Name())

			peerBestHash, err := handShake(ctx, status, rw, protocol, protocol, logger)
			if err != nil {
				// logger.Info("--> Handshake failed", "peerID", printablePeerID, "err", err)
				return err
			}

			// After successful handshake
			logger.Info("--> Handshake successful", "peerID", printablePeerID, "bestHash", peerBestHash.Hex())

			// handshake is successful
			logger.Info("[p2p] Received status message OK", "peerId", printablePeerID, "name", peer.Name())

			ss.GoodPeers.Store(peerID, peerInfo)
			ss.sendNewPeerToClients(gointerfaces.ConvertHashToH512(peerID))
			defer ss.sendGonePeerToClients(gointerfaces.ConvertHashToH512(peerID))
			getBlockHeadersErr := ss.getBlockHeaders(ctx, *peerBestHash, peerID)
			if getBlockHeadersErr != nil {
				return p2p.NewPeerError(p2p.PeerErrorFirstMessageSend, p2p.DiscNetworkError, getBlockHeadersErr, "p2p.Protocol.Run getBlockHeaders failure")
			}

			cap := p2p.Cap{Name: eth.ProtocolName, Version: protocol}

			return runPeer(
				ctx,
				peerID,
				cap,
				rw,
				peerInfo,
				ss.send,
				ss.hasSubscribers,
				logger,
			)
		},
		NodeInfo: func() interface{} {
			return readNodeInfo()
		},
		PeerInfo: func(peerID [64]byte) interface{} {
			// TODO: remember handshake reply per peer ID and return eth-related Status info (see ethPeerInfo in geth)
			return nil
		},
		//Attributes: []enr.Entry{eth.CurrentENREntry(chainConfig, genesisHash, headHeight)},
	})

	return ss
}

// Sentry creates and runs standalone sentry
func Sentry(ctx context.Context, dirs datadir.Dirs, sentryAddr string, discoveryDNS []string, cfg *p2p.Config, protocolVersion uint, healthCheck bool, logger log.Logger) error {
	dir.MustExist(dirs.DataDir)

	discovery := func() enode.Iterator {
		d, err := setupDiscovery(discoveryDNS)
		if err != nil {
			panic(err)
		}
		return d
	}
	cfg.DiscoveryDNS = discoveryDNS
	sentryServer := NewGrpcServer(ctx, discovery, func() *eth.NodeInfo { return nil }, cfg, protocolVersion, logger)

	grpcServer, err := grpcSentryServer(ctx, sentryAddr, sentryServer, healthCheck)
	if err != nil {
		return err
	}

	<-ctx.Done()
	grpcServer.GracefulStop()
	sentryServer.Close()
	return nil
}

type GrpcServer struct {
	proto_sentry.UnimplementedSentryServer
	ctx                  context.Context
	Protocols            []p2p.Protocol
	GoodPeers            sync.Map
	TxSubscribed         uint32 // Set to non-zero if downloader is subscribed to transaction messages
	p2pServer            *p2p.Server
	p2pServerLock        sync.RWMutex
	statusData           *proto_sentry.StatusData
	statusDataLock       sync.RWMutex
	messageStreams       map[proto_sentry.MessageId]map[uint64]chan *proto_sentry.InboundMessage
	messagesSubscriberID uint64
	messageStreamsLock   sync.RWMutex
	peersStreams         *PeersStreams
	p2p                  *p2p.Config
	logger               log.Logger
}

func (ss *GrpcServer) rangePeers(f func(peerInfo *PeerInfo) bool) {
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		return f(peerInfo)
	})
}

func (ss *GrpcServer) getPeer(peerID [64]byte) (peerInfo *PeerInfo) {
	if value, ok := ss.GoodPeers.Load(peerID); ok {
		peerInfo := value.(*PeerInfo)
		if peerInfo != nil {
			return peerInfo
		}
		ss.GoodPeers.Delete(peerID)
	}
	return nil
}

func (ss *GrpcServer) removePeer(peerID [64]byte, reason *p2p.PeerError) {
	if value, ok := ss.GoodPeers.LoadAndDelete(peerID); ok {
		peerInfo := value.(*PeerInfo)
		if peerInfo != nil {
			peerInfo.Remove(reason)
		}
	}
}

func (ss *GrpcServer) writePeer(logPrefix string, peerInfo *PeerInfo, msgcode uint64, data []byte, ttl time.Duration) {
	peerInfo.Async(func() {
		msgType := eth.ToProto[peerInfo.protocol][msgcode]
		trackPeerStatistics(peerInfo.peer.Fullname(), peerInfo.peer.ID().String(), false, msgType.String(), fmt.Sprintf("%s/%d", eth.ProtocolName, peerInfo.protocol), len(data))

		err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(data)), Payload: bytes.NewReader(data)})
		if err != nil {
			peerInfo.Remove(p2p.NewPeerError(p2p.PeerErrorMessageSend, p2p.DiscNetworkError, err, fmt.Sprintf("%s writePeer msgcode=%d", logPrefix, msgcode)))
			ss.GoodPeers.Delete(peerInfo.ID())
		} else {
			if ttl > 0 {
				peerInfo.AddDeadline(time.Now().Add(ttl))
			}
		}
	}, ss.logger)
}

func (ss *GrpcServer) getBlockHeaders(ctx context.Context, bestHash common.Hash, peerID [64]byte) error {
	fmt.Printf("-> getBlockHeaders::11 bestHash=%x peerID=%x\n", bestHash, peerID)
	if ss.Protocols[0].Version == 63 {
		req := &eth.GetBlockHeadersPacket{
			Amount:  1,
			Reverse: false,
			Skip:    0,
			Origin:  eth.HashOrNumber{Hash: bestHash},
		}
		b, err := rlp.EncodeToBytes(req)
		if err != nil {
			return fmt.Errorf("GrpcServer.getBlockHeaders encode ETH/63 packet failed: %w", err)
		}
		fmt.Printf("-> ETH/63 GetBlockHeaders → peer=%x hash=%x bytes=%d preview=%x\n",
			peerID[:8], bestHash, len(b), b[:min(32, len(b))])

		_, err = ss.SendMessageById(ctx, &proto_sentry.SendMessageByIdRequest{
			PeerId: gointerfaces.ConvertHashToH512(peerID),
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_63,
				Data: b,
			},
		})
		if err == nil {
			fmt.Printf("-> ETH/63 GetBlockHeaders sent OK to peer=%x\n", peerID[:8])
		} else {
			fmt.Printf("-> ETH/63 GetBlockHeaders send ERROR peer=%x err=%v\n", peerID[:8], err)
		}
		return err
	}

	req66 := &eth.GetBlockHeadersPacket66{
		RequestId: rand.Uint64(), // nolint: gosec
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Amount:  1,
			Reverse: false,
			Skip:    0,
			Origin:  eth.HashOrNumber{Hash: bestHash},
		},
	}
	b, err := rlp.EncodeToBytes(req66)
	if err != nil {
		return fmt.Errorf("GrpcServer.getBlockHeaders encode ETH/66 packet failed: %w", err)
	}
	fmt.Printf("-> ETH/66 GetBlockHeaders → peer=%x hash=%x rid=%d bytes=%d preview=%x\n",
		peerID[:8], bestHash, req66.RequestId, len(b), b[:min(32, len(b))])

	_, err = ss.SendMessageById(ctx, &proto_sentry.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: b,
		},
	})
	if err == nil {
		fmt.Printf("-> ETH/66 GetBlockHeaders sent OK to peer=%x\n", peerID[:8])
	} else {
		fmt.Printf("-> ETH/66 GetBlockHeaders send ERROR peer=%x err=%v\n", peerID[:8], err)
	}
	return err
}

func (ss *GrpcServer) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	peerID := ConvertH512ToPeerID(req.PeerId)
	peerInfo := ss.getPeer(peerID)
	if ss.statusData != nil && peerInfo != nil && !peerInfo.peer.Info().Network.Static && !peerInfo.peer.Info().Network.Trusted {
		ss.removePeer(peerID, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscRequested, nil, "penalized peer"))
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) PeerMinBlock(_ context.Context, req *proto_sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)
	if peerInfo := ss.getPeer(peerID); peerInfo != nil {
		peerInfo.SetIncreasedHeight(req.MinBlock)
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) findBestPeersWithPermit(peerCount int) []*PeerInfo {
	// Choose peer(s) that we can send this request to, with maximum number of permits
	now := time.Now()
	byMinBlock := make(PeersByMinBlock, 0, peerCount)
	var pokePeer *PeerInfo // Peer with the earliest dealine, to be "poked" by the request
	var pokeDeadline time.Time
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		deadlines := peerInfo.ClearDeadlines(now, false /* givePermit */)
		height := peerInfo.Height()
		//fmt.Printf("%d deadlines for peer %s\n", deadlines, peerID)
		if deadlines < maxPermitsPerPeer {
			heap.Push(&byMinBlock, PeerRef{pi: peerInfo, height: height})
			if byMinBlock.Len() > peerCount {
				// Remove the worst peer
				peerRef := heap.Pop(&byMinBlock).(PeerRef)
				latestDeadline := peerRef.pi.LatestDeadline()
				if pokePeer == nil || latestDeadline.Before(pokeDeadline) {
					pokeDeadline = latestDeadline
					pokePeer = peerInfo
				}
			}
		}
		return true
	})
	var foundPeers []*PeerInfo
	if peerCount == 1 || pokePeer == nil {
		foundPeers = make([]*PeerInfo, len(byMinBlock))
	} else {
		foundPeers = make([]*PeerInfo, len(byMinBlock)+1)
		foundPeers[len(foundPeers)-1] = pokePeer
	}
	for i, peerRef := range byMinBlock {
		foundPeers[i] = peerRef.pi
	}
	return foundPeers
}

func (ss *GrpcServer) findPeerByMinBlock(minBlock uint64) (*PeerInfo, bool) {
	// Choose a peer that we can send this request to, with maximum number of permits
	var foundPeerInfo *PeerInfo
	var maxPermits int
	now := time.Now()
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if peerInfo.Height() >= minBlock {
			deadlines := peerInfo.ClearDeadlines(now, false /* givePermit */)
			//fmt.Printf("%d deadlines for peer %s\n", deadlines, peerID)
			if deadlines < maxPermitsPerPeer {
				permits := maxPermitsPerPeer - deadlines
				if permits > maxPermits {
					maxPermits = permits
					foundPeerInfo = peerInfo
				}
			}
		}
		return true
	})
	return foundPeerInfo, maxPermits > 0
}

func (ss *GrpcServer) SendMessageByMinBlock(_ context.Context, inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	// Use messageCode to check all protocol versions, not just the first one
	msgcode, protocolVersions := ss.messageCode(inreq.Data.Id)
	if msgcode == 0 {
		// Message ID not found in any supported protocol version
		return reply, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s (supported protocols: %v)",
			inreq.Data.Id, ss.Protocols)
	}

	// Check if this message code is supported by SendMessageByMinBlock
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.GetBlockBodiesMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return reply, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s (msgcode: %d)",
			inreq.Data.Id, msgcode)
	}

	// Log which protocol versions support this message
	log.Debug("[SendMessageByMinBlock] MessageId: %s, MsgCode: %d, SupportedProtocolVersions: %v\n",
		inreq.Data.Id, msgcode, protocolVersions.ToSlice())

	if inreq.MaxPeers == 1 {
		peerInfo, found := ss.findPeerByMinBlock(inreq.MinBlock)
		if found {
			// Use the peer's protocol version for sending
			peerMsgcode, ok := eth.FromProto[peerInfo.protocol][inreq.Data.Id]
			if !ok {
				return reply, fmt.Errorf("message Id %s not supported by peer protocol %d",
					inreq.Data.Id, peerInfo.protocol)
			}
			ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, peerMsgcode, inreq.Data.Data, 30*time.Second)
			reply.Peers = []*proto_types.H512{gointerfaces.ConvertHashToH512(peerInfo.ID())}
			return reply, nil
		}
	}

	peerInfos := ss.findBestPeersWithPermit(int(inreq.MaxPeers))
	reply.Peers = make([]*proto_types.H512, len(peerInfos))
	for i, peerInfo := range peerInfos {
		// Use each peer's protocol version for sending
		peerMsgcode, ok := eth.FromProto[peerInfo.protocol][inreq.Data.Id]
		if !ok {
			// Skip peers that don't support this message ID
			continue
		}
		ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, peerMsgcode, inreq.Data.Data, 15*time.Second)
		reply.Peers[i] = gointerfaces.ConvertHashToH512(peerInfo.ID())
	}

	// Filter out nil peers (peers that didn't support the message)
	validPeers := make([]*proto_types.H512, 0, len(reply.Peers))
	for _, peer := range reply.Peers {
		if peer != nil {
			validPeers = append(validPeers, peer)
		}
	}
	reply.Peers = validPeers

	return reply, nil
}

func (ss *GrpcServer) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	peerID := ConvertH512ToPeerID(inreq.PeerId)
	peerInfo := ss.getPeer(peerID)
	if peerInfo == nil {
		//TODO: enable after support peer to sentry mapping
		//return reply, fmt.Errorf("peer not found: %s", peerID)
		return reply, nil
	}

	msgcode, ok := eth.FromProto[peerInfo.protocol][inreq.Data.Id]
	if !ok {
		return reply, fmt.Errorf("msgcode not found for message Id: %s (peer protocol %d)", inreq.Data.Id, peerInfo.protocol)
	}

	ss.writePeer("[sentry] sendMessageById", peerInfo, msgcode, inreq.Data.Data, 0)
	reply.Peers = []*proto_types.H512{inreq.PeerId}
	return reply, nil
}

func (ss *GrpcServer) messageCode(id proto_sentry.MessageId) (code uint64, protocolVersions mapset.Set[uint]) {
	protocolVersions = mapset.NewSet[uint]()
	for i := 0; i < len(ss.Protocols); i++ {
		version := ss.Protocols[i].Version
		if val, ok := eth.FromProto[version][id]; ok {
			code = val // assuming that the code doesn't change between protocol versions
			protocolVersions.Add(version)
		}
	}
	return
}

func (ss *GrpcServer) SendMessageToRandomPeers(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	msgcode, protocolVersions := ss.messageCode(req.Data.Id)
	ss.logger.Info("[NODE1] �� Sending message to random peers",
		"message_id", req.Data.Id.String(),
		"data_size", len(req.Data.Data),
		"max_peers", req.MaxPeers)

	if protocolVersions.Cardinality() == 0 ||
		(msgcode != eth.NewBlockMsg &&
			msgcode != eth.NewBlockHashesMsg &&
			msgcode != eth.NewPooledTransactionHashesMsg &&
			msgcode != eth.TransactionsMsg) {
		return reply, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	peerInfos := make([]*PeerInfo, 0, 100)
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if protocolVersions.Contains(peerInfo.protocol) {
			peerInfos = append(peerInfos, peerInfo)
		}
		return true
	})
	rand.Shuffle(len(peerInfos), func(i int, j int) {
		peerInfos[i], peerInfos[j] = peerInfos[j], peerInfos[i]
	})

	var peersToSendCount int
	if req.MaxPeers > 0 {
		peersToSendCount = int(math.Min(float64(req.MaxPeers), float64(len(peerInfos))))
	} else {
		// MaxPeers == 0 means send to all
		peersToSendCount = len(peerInfos)
	}

	// Add logging for peer selection
	ss.logger.Info("[NODE1] �� Selected peers for message",
		"total_peers", len(peerInfos),
		"selected_peers", peersToSendCount,
		"message_type", req.Data.Id.String())

	// Send the block to a subset of our peers at random
	for _, peerInfo := range peerInfos[:peersToSendCount] {
		ss.writePeer("[sentry] sendMessageToRandomPeers", peerInfo, msgcode, req.Data.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
		ss.logger.Info("[NODE1] 📤 Sent message to peer",
			"peer_id", peerInfo.ID(),
			"message_type", req.Data.Id.String(),
			"data_size", len(req.Data.Data))
	}
	ss.logger.Info("[NODE1] ✅ Successfully sent message to peers",
		"total_sent", len(reply.Peers),
		"message_type", req.Data.Id.String())
	return reply, nil
}

func (ss *GrpcServer) SendMessageToRandomPeers63(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	fmt.Printf("-> SendMessageToRandomPeers63 request received: %+v\n", req)
	reply := &proto_sentry.SentPeers{}

	return reply, nil
}

func (ss *GrpcServer) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	msgcode, protocolVersions := ss.messageCode(req.Id)

	// If messageCode didn't find it, try checking ETH/63 explicitly for ETH/63 message IDs
	if protocolVersions.Cardinality() == 0 {
		// Check if this is an ETH/63 message that we should handle
		if msgcode63, ok := eth.FromProto[direct.ETH63][req.Id]; ok {
			msgcode = msgcode63
			protocolVersions.Add(direct.ETH63)
		}
	}

	if protocolVersions.Cardinality() == 0 ||
		(msgcode != eth.NewBlockMsg &&
			msgcode != eth.NewPooledTransactionHashesMsg && // to broadcast new local transactions
			msgcode != eth.NewBlockHashesMsg) {
		return reply, fmt.Errorf("sendMessageToAll not implemented for message Id: %s (msgcode: %d, protocolVersions: %v)", req.Id, msgcode, protocolVersions.ToSlice())
	}

	var lastErr error
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		// For each peer, use their protocol version to get the correct msgcode
		peerMsgcode, ok := eth.FromProto[peerInfo.protocol][req.Id]
		if !ok {
			// Skip peers that don't support this message ID
			return true
		}
		ss.writePeer("[sentry] SendMessageToAll", peerInfo, peerMsgcode, req.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
		return true
	})
	return reply, lastErr
}

func (ss *GrpcServer) HandShake(context.Context, *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	reply := &proto_sentry.HandShakeReply{}
	fmt.Printf("-> Received handshake request from ETH client\n")

	switch ss.Protocols[0].Version {
	case direct.ETH63:
		ss.logger.Info("-> Handshake with ETH/63 peer")
		reply.Protocol = proto_sentry.Protocol_ETH63
	case direct.ETH67:
		ss.logger.Info("-> Handshake with ETH/67 peer\n")
		reply.Protocol = proto_sentry.Protocol_ETH67
	case direct.ETH68:
		ss.logger.Info("-> Handshake with ETH/68 peer\n")
		reply.Protocol = proto_sentry.Protocol_ETH68
	}

	ss.logger.Info("-> Handshake response: protocol=%s\n", reply.Protocol.String())
	return reply, nil
}

func (ss *GrpcServer) startP2PServer(genesisHash common.Hash) (*p2p.Server, error) {
	if !ss.p2p.NoDiscovery {
		if len(ss.p2p.DiscoveryDNS) == 0 {
			if url := ss.p2p.LookupDNSNetwork(genesisHash, "all"); url != "" {
				ss.p2p.DiscoveryDNS = []string{url}
			}

			for _, p := range ss.Protocols {
				dialCandidates, err := setupDiscovery(ss.p2p.DiscoveryDNS)
				if err != nil {
					return nil, err
				}
				p.DialCandidates = dialCandidates
			}
		}
	}

	srv, err := makeP2PServer(*ss.p2p, genesisHash, ss.Protocols)
	if err != nil {
		return nil, err
	}

	if err = srv.Start(ss.ctx, ss.logger); err != nil {
		srv.Stop()
		return nil, fmt.Errorf("could not start server: %w", err)
	}

	return srv, nil
}

func (ss *GrpcServer) getP2PServer() *p2p.Server {
	ss.p2pServerLock.RLock()
	defer ss.p2pServerLock.RUnlock()
	return ss.p2pServer
}

func (ss *GrpcServer) SetStatus(ctx context.Context, statusData *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	reply := &proto_sentry.SetStatusReply{}

	ss.p2pServerLock.Lock()
	defer ss.p2pServerLock.Unlock()
	if ss.p2pServer == nil {
		srv, err := ss.startP2PServer(genesisHash)
		if err != nil {
			return reply, err
		}
		ss.p2pServer = srv
	}

	ss.statusDataLock.Lock()
	defer ss.statusDataLock.Unlock()

	ss.p2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.HeightForks, statusData.ForkData.TimeForks, genesisHash, statusData.MaxBlockHeight, statusData.MaxBlockTime))
	if ss.statusData == nil || statusData.MaxBlockHeight != 0 {
		// Not overwrite statusData if the message contains zero MaxBlock (comes from standalone transaction pool)
		ss.statusData = statusData
	}
	return reply, nil
}

func (ss *GrpcServer) Peers(_ context.Context, _ *emptypb.Empty) (*proto_sentry.PeersReply, error) {
	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	peers := p2pServer.PeersInfo()

	var reply proto_sentry.PeersReply
	reply.Peers = make([]*proto_types.PeerInfo, 0, len(peers))

	for _, peer := range peers {
		rpcPeer := proto_types.PeerInfo{
			Id:             peer.ID,
			Name:           peer.Name,
			Enode:          peer.Enode,
			Enr:            peer.ENR,
			Caps:           peer.Caps,
			ConnLocalAddr:  peer.Network.LocalAddress,
			ConnRemoteAddr: peer.Network.RemoteAddress,
			ConnIsInbound:  peer.Network.Inbound,
			ConnIsTrusted:  peer.Network.Trusted,
			ConnIsStatic:   peer.Network.Static,
		}
		reply.Peers = append(reply.Peers, &rpcPeer)
	}

	return &reply, nil
}

func (ss *GrpcServer) SimplePeerCount() map[uint]int {
	counts := map[uint]int{}
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		counts[peerInfo.protocol]++
		return true
	})
	return counts
}

func (ss *GrpcServer) PeerCount(_ context.Context, req *proto_sentry.PeerCountRequest) (*proto_sentry.PeerCountReply, error) {
	counts := ss.SimplePeerCount()
	reply := &proto_sentry.PeerCountReply{}
	for protocol, count := range counts {
		reply.Count += uint64(count)
		reply.CountsPerProtocol = append(reply.CountsPerProtocol, &proto_sentry.PeerCountPerProtocol{Protocol: proto_sentry.Protocol(protocol), Count: uint64(count)})
	}
	return reply, nil
}

func (ss *GrpcServer) PeerById(_ context.Context, req *proto_sentry.PeerByIdRequest) (*proto_sentry.PeerByIdReply, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)

	var rpcPeer *proto_types.PeerInfo
	sentryPeer := ss.getPeer(peerID)

	if sentryPeer != nil {
		peer := sentryPeer.peer.Info()
		rpcPeer = &proto_types.PeerInfo{
			Id:             peer.ID,
			Name:           peer.Name,
			Enode:          peer.Enode,
			Enr:            peer.ENR,
			Caps:           peer.Caps,
			ConnLocalAddr:  peer.Network.LocalAddress,
			ConnRemoteAddr: peer.Network.RemoteAddress,
			ConnIsInbound:  peer.Network.Inbound,
			ConnIsTrusted:  peer.Network.Trusted,
			ConnIsStatic:   peer.Network.Static,
		}
	}

	return &proto_sentry.PeerByIdReply{Peer: rpcPeer}, nil
}

// setupDiscovery creates the node discovery source for the `eth` protocol.
func setupDiscovery(urls []string) (enode.Iterator, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	client := dnsdisc.NewClient(dnsdisc.Config{})
	return client.NewIterator(urls...)
}

func (ss *GrpcServer) GetStatus() *proto_sentry.StatusData {
	ss.statusDataLock.RLock()
	defer ss.statusDataLock.RUnlock()
	return ss.statusData
}

func (ss *GrpcServer) send(msgID proto_sentry.MessageId, peerID [64]byte, b []byte) {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	req := &proto_sentry.InboundMessage{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Id:     msgID,
		Data:   b,
	}
	subscribers := ss.messageStreams[msgID]
	subscriberCount := 0
	if subscribers != nil {
		subscriberCount = len(subscribers)
	}
	// fmt.Printf("-> send(): msgID=%s peer=%x bytes=%d subscribers=%d\n", msgID.String(), peerID[:8], len(b), subscriberCount)
	if subscriberCount == 0 {
		fmt.Printf("-> WARNING: send() called but NO SUBSCRIBERS for %s! Message will be dropped!\n", msgID.String())
		return
	}
	for i := range ss.messageStreams[msgID] {
		ch := ss.messageStreams[msgID][i]
		// fmt.Printf("-> send(): Sending to subscriber channel %p (subscriber %d)\n", ch, i)
		ch <- req
		if len(ch) > MessagesQueueSize/2 {
			ss.logger.Debug("[sentry] consuming is slow, drop 50% of old messages", "msgID", msgID.String())
			// evict old messages from channel
			for j := 0; j < MessagesQueueSize/4; j++ {
				select {
				case <-ch:
				default:
				}
			}
		}
	}
	// fmt.Printf("-> send(): Successfully sent %s to %d subscribers\n", msgID.String(), subscriberCount)
}

func (ss *GrpcServer) hasSubscribers(msgID proto_sentry.MessageId) bool {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	ok := ss.messageStreams[msgID] != nil && len(ss.messageStreams[msgID]) > 0
	return ok
}

func (ss *GrpcServer) addMessagesStream(ids []proto_sentry.MessageId, ch chan *proto_sentry.InboundMessage) func() {
	ss.messageStreamsLock.Lock()
	defer ss.messageStreamsLock.Unlock()
	if ss.messageStreams == nil {
		ss.messageStreams = map[proto_sentry.MessageId]map[uint64]chan *proto_sentry.InboundMessage{}
	}

	ss.messagesSubscriberID++

	fmt.Printf("-> addMessagesStream: subscriberID=%d, subscribing to %d message IDs:\n", ss.messagesSubscriberID, len(ids))
	for _, id := range ids {
		fmt.Printf("   - %s\n", id.String())
		m, ok := ss.messageStreams[id]
		if !ok {
			m = map[uint64]chan *proto_sentry.InboundMessage{}
			ss.messageStreams[id] = m
			fmt.Printf("-> Created new message stream map for ID: %s\n", id.String())
		}
		m[ss.messagesSubscriberID] = ch
		fmt.Printf("-> Added subscriber %d to %s (total subscribers now: %d)\n", ss.messagesSubscriberID, id.String(), len(m))
	}

	sID := ss.messagesSubscriberID
	return func() {
		ss.messageStreamsLock.Lock()
		defer ss.messageStreamsLock.Unlock()
		for _, id := range ids {
			delete(ss.messageStreams[id], sID)
		}
	}
}

const MessagesQueueSize = 1024 // one such queue per client of .Messages stream
func (ss *GrpcServer) Messages(req *proto_sentry.MessagesRequest, stream proto_sentry.Sentry_MessagesServer) error {
	// Log what we received
	fmt.Printf("-> Messages() called with req.Ids length=%d\n", len(req.Ids))
	if len(req.Ids) > 0 {
		fmt.Printf("-> Received IDs:\n")
		for i, id := range req.Ids {
			fmt.Printf("   [%d] ID=%d (%s)\n", i, int32(id), id.String())
		}
	} else {
		fmt.Printf("-> WARNING: req.Ids is EMPTY!\n")
		fmt.Printf("-> req pointer: %p\n", req)
		fmt.Printf("-> req.Ids pointer: %p\n", req.Ids)
	}

	ch := make(chan *proto_sentry.InboundMessage, MessagesQueueSize)
	defer close(ch)
	clean := ss.addMessagesStream(req.Ids, ch)
	defer clean()

	for {
		select {
		case <-ss.ctx.Done():
			return nil
		case <-stream.Context().Done():
			return nil
		case in := <-ch:
			if err := stream.Send(in); err != nil {
				fmt.Println("[sentry] Sending msg to core P2P failed", "msg", in.Id.String(), "err", err)
				ss.logger.Warn("Sending msg to core P2P failed", "msg", in.Id.String(), "err", err)
				return err
			}
		}
	}
}

// func (ss *GrpcServer) isETH63Message(id proto_sentry.MessageId) bool {
// 	eth63Messages := []proto_sentry.MessageId{
// 		proto_sentry.MessageId_STATUS_63,
// 		proto_sentry.MessageId_GET_BLOCK_HEADERS_63,
// 		proto_sentry.MessageId_BLOCK_HEADERS_63,
// 		proto_sentry.MessageId_GET_BLOCK_BODIES_63,
// 		proto_sentry.MessageId_BLOCK_BODIES_63,
// 		proto_sentry.MessageId_GET_RECEIPTS_63,
// 		proto_sentry.MessageId_RECEIPTS_63,
// 		proto_sentry.MessageId_NEW_BLOCK_63,
// 		proto_sentry.MessageId_TRANSACTIONS_63,
// 	}

// 	for _, msgID := range eth63Messages {
// 		if id == msgID {
// 			return true
// 		}
// 	}
// 	return false
// }

// isETH63Message checks if the message type is part of the ETH/63 protocol
func isETH63Message(msgCode uint64) bool {
	eth63MsgCodes := map[uint64]struct{}{
		eth.StatusMsg:          {},
		eth.NewBlockHashesMsg:  {},
		eth.TransactionsMsg:    {},
		eth.GetBlockHeadersMsg: {},
		eth.BlockHeadersMsg:    {},
		eth.GetBlockBodiesMsg:  {},
		eth.BlockBodiesMsg:     {},
		eth.NewBlockMsg:        {},
		eth.GetReceiptsMsg:     {},
		eth.ReceiptsMsg:        {},
	}
	_, isETH63 := eth63MsgCodes[msgCode]
	return isETH63
}

// Close performs cleanup operations for the sentry
func (ss *GrpcServer) Close() {
	p2pServer := ss.getP2PServer()
	if p2pServer != nil {
		p2pServer.Stop()
	}
}

func (ss *GrpcServer) sendNewPeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Connect}); err != nil {
		ss.logger.Warn("Sending new peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) sendGonePeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Disconnect}); err != nil {
		ss.logger.Warn("Sending gone peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) PeerEvents(req *proto_sentry.PeerEventsRequest, server proto_sentry.Sentry_PeerEventsServer) error {
	clean := ss.peersStreams.Add(server)
	defer clean()
	select {
	case <-ss.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

func (ss *GrpcServer) AddPeer(_ context.Context, req *proto_sentry.AddPeerRequest) (*proto_sentry.AddPeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.AddPeer(node)

	return &proto_sentry.AddPeerReply{Success: true}, nil
}

func (ss *GrpcServer) NodeInfo(_ context.Context, _ *emptypb.Empty) (*proto_types.NodeInfoReply, error) {
	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	info := p2pServer.NodeInfo()
	ret := &proto_types.NodeInfoReply{
		Id:    info.ID,
		Name:  info.Name,
		Enode: info.Enode,
		Enr:   info.ENR,
		Ports: &proto_types.NodeInfoPorts{
			Discovery: uint32(info.Ports.Discovery),
			Listener:  uint32(info.Ports.Listener),
		},
		ListenerAddr: info.ListenAddr,
	}

	protos, err := json.Marshal(info.Protocols)
	if err != nil {
		return nil, fmt.Errorf("cannot encode protocols map: %w", err)
	}

	ret.Protocols = protos
	return ret, nil
}

// PeersStreams - it's safe to use this class as non-pointer
type PeersStreams struct {
	mu      sync.RWMutex
	id      uint
	streams map[uint]proto_sentry.Sentry_PeerEventsServer
}

func NewPeersStreams() *PeersStreams {
	return &PeersStreams{}
}

func (s *PeersStreams) Add(stream proto_sentry.Sentry_PeerEventsServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]proto_sentry.Sentry_PeerEventsServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *PeersStreams) doBroadcast(reply *proto_sentry.PeerEvent) (ids []uint, errs []error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, stream := range s.streams {
		err := stream.Send(reply)
		if err != nil {
			select {
			case <-stream.Context().Done():
				ids = append(ids, id)
			default:
			}
			errs = append(errs, err)
		}
	}
	return
}

func (s *PeersStreams) Broadcast(reply *proto_sentry.PeerEvent) (errs []error) {
	var ids []uint
	ids, errs = s.doBroadcast(reply)
	if len(ids) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	for _, id := range ids {
		delete(s.streams, id)
	}
	return errs
}

func (s *PeersStreams) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

func (s *PeersStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.streams[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.streams, id)
}
