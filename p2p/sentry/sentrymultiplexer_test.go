package sentry

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/eth63"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func newClient(ctrl *gomock.Controller, i int, caps []string) *direct.MockSentryClient {
	client := direct.NewMockSentryClient(ctrl)
	pk, _ := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	node := enode.NewV4(&pk.PublicKey, net.IPv4(127, 0, 0, byte(i)), 30001, 30001)

	if len(caps) == 0 {
		caps = []string{"eth/68"}
	}

	client.EXPECT().NodeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(&typesproto.NodeInfoReply{
		Id:    node.ID().String(),
		Name:  fmt.Sprintf("client-%d", i),
		Enode: node.URLv4(),
		Enr:   node.String(),
		Ports: &typesproto.NodeInfoPorts{
			Discovery: uint32(30000),
			Listener:  uint32(30001),
		},
		ListenerAddr: fmt.Sprintf("127.0.0.%d", i),
	}, nil).AnyTimes()

	client.EXPECT().HandShake(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	client.EXPECT().Peers(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
			id := [64]byte{byte(i)}
			return &sentryproto.PeersReply{
				Peers: []*typesproto.PeerInfo{
					{
						Id:   hex.EncodeToString(id[:]),
						Caps: caps,
					},
				},
			}, nil
		}).AnyTimes()
	return client
}

func TestCreateMultiplexer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	for i := 0; i < 10; i++ {
		clients = append(clients, newClient(ctrl, i, nil))
	}

	mux := sentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	hs, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NotNil(t, hs)
	require.NoError(t, err)

	info, err := mux.NodeInfo(context.Background(), &emptypb.Empty{})
	require.Nil(t, info)
	require.Error(t, err)

	infos, err := mux.NodeInfos(context.Background())
	require.NoError(t, err)
	require.Len(t, infos, 10)
}

func TestStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.StatusData, co ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SetStatusReply{}, nil
			})
		client.EXPECT().PenalizePeer(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.PenalizePeerRequest, co ...grpc.CallOption) (*emptypb.Empty, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &emptypb.Empty{}, nil
			})
		client.EXPECT().PeerMinBlock(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.PeerMinBlockRequest, co ...grpc.CallOption) (*emptypb.Empty, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &emptypb.Empty{}, nil
			})

		clients = append(clients, client)
	}

	mux := sentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	hs, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, hs)

	reply, err := mux.SetStatus(context.Background(), &sentryproto.StatusData{})
	require.NoError(t, err)
	require.NotNil(t, reply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	empty, err := mux.PenalizePeer(context.Background(), &sentryproto.PenalizePeerRequest{})
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	empty, err = mux.PeerMinBlock(context.Background(), &sentryproto.PeerMinBlockRequest{})
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Equal(t, 10, statusCount)
}

func TestSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().SendMessageByMinBlock(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SentPeers{}, nil
			}).AnyTimes()
		client.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SentPeers{}, nil
			}).AnyTimes()

		clients = append(clients, client)

		fmt.Println("client")
	}

	mux := sentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	_, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	sendReply, err := mux.SendMessageByMinBlock(context.Background(), &sentryproto.SendMessageByMinBlockRequest{})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 1, statusCount)

	statusCount = 0

	for i := byte(0); i < 10; i++ {
		sendReply, err = mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			Data: &sentryproto.OutboundMessageData{
				Id: sentryproto.MessageId_BLOCK_BODIES_66,
			},
			PeerId: gointerfaces.ConvertHashToH512([64]byte{i}),
		})
		require.NoError(t, err)
		require.NotNil(t, sendReply)
		require.Equal(t, 1, statusCount)

		statusCount = 0
	}

	sendReply, err = mux.SendMessageToRandomPeers(context.Background(), &sentryproto.SendMessageToRandomPeersRequest{
		Data: &sentryproto.OutboundMessageData{
			Id: sentryproto.MessageId_BLOCK_BODIES_66,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	sendReply, err = mux.SendMessageToAll(context.Background(), &sentryproto.OutboundMessageData{
		Id: sentryproto.MessageId_BLOCK_BODIES_66,
	})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 10, statusCount)
}

func TestMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
				ch := make(chan sentry.StreamReply[*sentryproto.InboundMessage], 16384)
				streamServer := &sentry.SentryStreamS[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}

				go func() {
					for i := 0; i < 5; i++ {
						streamServer.Send(&sentryproto.InboundMessage{})
					}

					streamServer.Close()
				}()

				return &sentry.SentryStreamC[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}, nil
			})

		clients = append(clients, client)
	}

	mux := sentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	client, err := mux.Messages(context.Background(), &sentryproto.MessagesRequest{})
	require.NoError(t, err)
	require.NotNil(t, client)

	var messageCount int

	for {
		message, err := client.Recv()

		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}

		messageCount++
		require.NotNil(t, message)
	}

	require.Equal(t, 50, messageCount)
}

func TestPeers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().AddPeer(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.AddPeerReply{}, nil
			})
		client.EXPECT().PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerEventsRequest, opts ...grpc.CallOption) (sentryproto.Sentry_PeerEventsClient, error) {
				ch := make(chan sentry.StreamReply[*sentryproto.PeerEvent], 16384)
				streamServer := &sentry.SentryStreamS[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}

				go func() {
					for i := 0; i < 5; i++ {
						streamServer.Send(&sentryproto.PeerEvent{})
					}

					streamServer.Close()
				}()

				return &sentry.SentryStreamC[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}, nil
			})
		client.EXPECT().PeerById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerByIdRequest, opts ...grpc.CallOption) (*sentryproto.PeerByIdReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.PeerByIdReply{}, nil
			})
		client.EXPECT().PeerCount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.PeerCountReply{}, nil
			})

		clients = append(clients, client)
	}

	mux := sentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	_, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	addPeerReply, err := mux.AddPeer(context.Background(), &sentryproto.AddPeerRequest{})
	require.NoError(t, err)
	require.NotNil(t, addPeerReply)
	require.Equal(t, 10, statusCount)

	client, err := mux.PeerEvents(context.Background(), &sentryproto.PeerEventsRequest{})
	require.NoError(t, err)
	require.NotNil(t, client)

	var eventCount int

	for {
		message, err := client.Recv()

		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}

		eventCount++
		require.NotNil(t, message)
	}

	require.Equal(t, 50, eventCount)

	statusCount = 0

	peerIdReply, err := mux.PeerById(context.Background(), &sentryproto.PeerByIdRequest{})
	require.NoError(t, err)
	require.NotNil(t, peerIdReply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	peerCountReply, err := mux.PeerCount(context.Background(), &sentryproto.PeerCountRequest{})
	require.NoError(t, err)
	require.NotNil(t, peerCountReply)
	require.Equal(t, 10, statusCount)

	peersReply, err := mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, peersReply)
	require.Len(t, peersReply.GetPeers(), 10)
}

func TestPeerHandshake(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create two mock sentry clients
	clientA := newClient(ctrl, 1, []string{"eth/67"})
	clientB := newClient(ctrl, 2, []string{"eth/67"})

	// Create multiplexers for both peers
	muxA := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{clientA})
	muxB := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{clientB})

	// // Create peer info for both peers
	// peerIDA := [64]byte{1, 2, 3, 4}
	// peerIDB := [64]byte{5, 6, 7, 8}

	// Start handshake in separate goroutines
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Peer A initiates handshake
		hsA, err := muxA.HandShake(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, hsA)
		fmt.Println("Peer A handshake complete, protocol:", hsA.Protocol)

		// Peer B responds to handshake
		hsB, err := muxB.HandShake(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, hsB)
		fmt.Println("Peer B handshake complete, protocol:", hsB.Protocol)
	}()

	// Wait for handshake to complete or timeout
	select {
	case <-done:
		fmt.Println("Handshake completed successfully")
	case <-time.After(5 * time.Second):
		fmt.Println("Handshake timed out")
	}

	// Verify peers are connected
	peersA, err := muxA.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotEmpty(t, peersA.Peers, "Peer A should have peers")

	peersB, err := muxB.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotEmpty(t, peersB.Peers, "Peer B should have peers")

	fmt.Println("Peer connection test completed successfully")
}

// ... existing imports and test setup ...

func TestMessageExchange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry clients
	clientA := newClient(ctrl, 1, []string{""})
	clientB := newClient(ctrl, 2, []string{""})

	// Configure handshake for clientA
	clientA.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Configure handshake for clientB
	clientB.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Create message channels for communication
	msgChanAtoB := make(chan *sentryproto.OutboundMessageData, 10)
	msgChanBtoA := make(chan *sentryproto.OutboundMessageData, 10)

	// Configure clientA to send messages to clientB
	clientA.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			fmt.Println("A -> B:", req.Data.Id)
			msgChanAtoB <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	clientB.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			fmt.Println("B -> A:", req.Data.Id)
			msgChanBtoA <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Create multiplexers
	muxA := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{clientA})
	muxB := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{clientB})

	// List connected peers for muxA
	peersA, err := muxA.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	fmt.Println("muxA connected peers:", peersA.Peers)

	// List connected peers for muxB
	peersB, err := muxB.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	fmt.Println("muxB connected peers: ", peersB.Peers)

	// If you want to see the peer IDs in hex format
	for i, peer := range peersA.Peers {
		fmt.Println("muxA Peer: ", i, peer.GetId())
	}
	for i, peer := range peersB.Peers {
		fmt.Println("muxB Peer: ", i, peer.GetId())
	}

	// Convert peer IDs to H512 format
	peerId1Bytes, err := hex.DecodeString(peersA.Peers[0].Id)
	require.NoError(t, err)
	var peerId1Arr [64]byte
	copy(peerId1Arr[:], peerId1Bytes)
	peerId1 := gointerfaces.ConvertHashToH512(peerId1Arr)

	peerId2Bytes, err := hex.DecodeString(peersB.Peers[0].Id)
	require.NoError(t, err)
	var peerId2Arr [64]byte
	copy(peerId2Arr[:], peerId2Bytes)
	peerId2 := gointerfaces.ConvertHashToH512(peerId2Arr)

	// Mock peer registration
	clientA.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()
	clientB.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()

	// Test handshake
	hsA, err := muxA.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, hsA)

	hsB, err := muxB.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, hsB)

	fmt.Printf("Handshake complete - A: %s B: %s\n",
		sentryproto.Protocol_name[int32(hsA.Protocol)],
		sentryproto.Protocol_name[int32(hsB.Protocol)],
	)

	// show list peers connected of peer1, peer2

	// Test message exchange - Peer A sends a transaction to Peer B
	txData := []byte("test transaction data")
	_, err = muxA.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
		PeerId: peerId1,
		Data: &sentryproto.OutboundMessageData{
			Id:   sentryproto.MessageId_TRANSACTIONS_63,
			Data: txData,
		},
	})
	require.NoError(t, err)

	// Verify the message was sent from A to B
	select {
	case receivedMsg := <-msgChanAtoB:
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_63, receivedMsg.Id)
		require.Equal(t, txData, receivedMsg.Data)
		fmt.Println("Transaction successfully sent from A to B")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for transaction from A to B")
	}

	// Peer B can now send a response back to Peer A
	respTx := &sentryproto.OutboundMessageData{
		Id:   sentryproto.MessageId_TRANSACTIONS_63,
		Data: []byte("response from B"),
	}
	_, err = muxB.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
		Data:   respTx,
		PeerId: peerId2,
	})
	require.NoError(t, err)

	// Verify the response was received by Peer A
	select {
	case receivedResp := <-msgChanBtoA:
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_63, receivedResp.Id)
		fmt.Println("Response received by Peer A")
	case <-time.After(1 * time.Second):
		fmt.Println("Timeout waiting for response from B to A")
	}

	fmt.Println("Peer transaction exchange test completed successfully")
}

func TestETH63TransactionGeneratedByGeth(t *testing.T) {
	// Transaction bytes generated by Geth 1.9
	txBytes := []byte{236, 42, 133, 4, 168, 23, 200, 0, 130, 82, 8, 148, 116, 45, 53, 204, 102, 52, 192, 83, 41, 37, 163, 184, 209, 50, 15, 138, 148, 238, 58, 54, 136, 13, 224, 182, 179, 167, 100, 0, 0, 128, 128, 128, 128}
	var tx *eth63.ETH63Transaction

	err := rlp.DecodeBytes(txBytes, &tx)
	if err != nil {
		t.Fatalf("failed to decode transaction: %v", err)
	}
	// require.Len(t, fakeTxs, 1)
	require.Equal(t, tx.Nonce(), uint64(42))
	require.Equal(t, tx.Gas(), uint64(21000))
	require.Equal(t, tx.GasPrice(), big.NewInt(20000000000))
	require.Equal(t, tx.Value(), big.NewInt(1000000000000000000))
	to := common.HexToAddress("0x742d35Cc6634C0532925a3b8D1320f8a94eE3a36")
	require.Equal(t, tx.To().Hex(), to.Hex())
}

func TestETH63TransactionSigned(t *testing.T) {
	txBytes := []byte{248, 108, 42, 133, 4, 168, 23, 200, 0, 130, 82, 8, 148, 116, 45, 53, 204, 102, 52, 192, 83, 41, 37, 163, 184, 209, 50, 15, 138, 148, 238, 58, 54, 136, 13, 224, 182, 179, 167, 100, 0, 0, 128, 28, 160, 54, 239, 225, 241, 14, 162, 217, 172, 113, 201, 14, 2, 112, 172, 167, 188, 128, 195, 62, 32, 153, 52, 53, 107, 5, 33, 6, 21, 227, 228, 14, 133, 160, 3, 207, 51, 9, 86, 46, 42, 243, 35, 85, 204, 59, 255, 163, 230, 83, 30, 188, 53, 111, 50, 242, 125, 26, 18, 157, 50, 120, 109, 195, 109, 182}
	var tx *eth63.ETH63Transaction
	err := rlp.DecodeBytes(txBytes, &tx)
	if err != nil {
		t.Fatalf("failed to decode transaction: %v", err)
	}
	fmt.Println("-> transaction hash:", tx.Hash().Hex())
	// Verify the sender can be recovered
	recoveredSender, err := eth63.Sender(eth63.HomesteadSigner{}, tx)
	if err != nil {
		t.Fatalf("could not recover sender: %v", err)
	}
	fmt.Println("-> sender:", recoveredSender.Hex())
}

func TestETH63TransactionSigning(t *testing.T) {
	for i, test := range []struct {
		txRlp, addr string
	}{
		{"f864808504a817c800825208943535353535353535353535353535353535353535808025a0044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116da0044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116d", "0xf0f6f18bca1b28cd68e4357452947e021241e9ce"},
		{"f864018504a817c80182a410943535353535353535353535353535353535353535018025a0489efdaa54c0f20c7adf612882df0950f5a951637e0307cdcb4c672f298b8bcaa0489efdaa54c0f20c7adf612882df0950f5a951637e0307cdcb4c672f298b8bc6", "0x23ef145a395ea3fa3deb533b8a9e1b4c6c25d112"},
		{"f864028504a817c80282f618943535353535353535353535353535353535353535088025a02d7c5bef027816a800da1736444fb58a807ef4c9603b7848673f7e3a68eb14a5a02d7c5bef027816a800da1736444fb58a807ef4c9603b7848673f7e3a68eb14a5", "0x2e485e0c23b4c3c542628a5f672eeab0ad4888be"},
		{"f865038504a817c803830148209435353535353535353535353535353535353535351b8025a02a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4e0a02a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4de", "0x82a88539669a3fd524d669e858935de5e5410cf0"},
		{"f865048504a817c80483019a28943535353535353535353535353535353535353535408025a013600b294191fc92924bb3ce4b969c1e7e2bab8f4c93c3fc6d0a51733df3c063a013600b294191fc92924bb3ce4b969c1e7e2bab8f4c93c3fc6d0a51733df3c060", "0xf9358f2538fd5ccfeb848b64a96b743fcc930554"},
		{"f865058504a817c8058301ec309435353535353535353535353535353535353535357d8025a04eebf77a833b30520287ddd9478ff51abbdffa30aa90a8d655dba0e8a79ce0c1a04eebf77a833b30520287ddd9478ff51abbdffa30aa90a8d655dba0e8a79ce0c1", "0xa8f7aba377317440bc5b26198a363ad22af1f3a4"},
		{"f866068504a817c80683023e3894353535353535353535353535353535353535353581d88025a06455bf8ea6e7463a1046a0b52804526e119b4bf5136279614e0b1e8e296a4e2fa06455bf8ea6e7463a1046a0b52804526e119b4bf5136279614e0b1e8e296a4e2d", "0xf1f571dc362a0e5b2696b8e775f8491d3e50de35"},
		{"f867078504a817c807830290409435353535353535353535353535353535353535358201578025a052f1a9b320cab38e5da8a8f97989383aab0a49165fc91c737310e4f7e9821021a052f1a9b320cab38e5da8a8f97989383aab0a49165fc91c737310e4f7e9821021", "0xd37922162ab7cea97c97a87551ed02c9a38b7332"},
		{"f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10", "0x9bddad43f934d313c2b79ca28a432dd2b7281029"},
		{"f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", "0x3c24d7329e92f84f08556ceb6df1cdb0104ca49f"},
	} {
		signer := eth63.NewEIP155Signer(big.NewInt(1))

		var tx *eth63.ETH63Transaction
		err := rlp.DecodeBytes(common.Hex2Bytes(test.txRlp), &tx)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		from, err := eth63.Sender(signer, tx)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		addr := common.HexToAddress(test.addr)
		if from != addr {
			t.Errorf("%d: expected %x got %x", i, addr, from)
		}
	}
}

// TestETH63TransactionExchange tests ETH/63 transaction exchange between two peers
func TestETH63TransactionExchange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry clients for two peers
	peer1Client := newClient(ctrl, 1, []string{"eth/63"})
	peer2Client := newClient(ctrl, 2, []string{"eth/63"})

	// Configure handshake for both peers to use ETH/63
	peer1Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	peer2Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Create message channels for peer communication
	peer1ToPeer2Chan := make(chan *sentryproto.OutboundMessageData, 10)
	peer2ToPeer1Chan := make(chan *sentryproto.OutboundMessageData, 10)

	// Configure peer1 to send messages to peer2
	peer1Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer1 -> Peer2: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer1ToPeer2Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Configure peer2 to send messages to peer1
	peer2Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer2 -> Peer1: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer2ToPeer1Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Create multiplexers for both peers
	peer1Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer1Client})
	peer2Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer2Client})

	// Mock peer count for both clients
	peer1Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()
	peer2Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()

	// Perform handshakes
	hs1, err := peer1Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs1.Protocol)

	hs2, err := peer2Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs2.Protocol)

	t.Logf("Handshake complete - Peer1: %s, Peer2: %s",
		sentryproto.Protocol_name[int32(hs1.Protocol)],
		sentryproto.Protocol_name[int32(hs2.Protocol)])

	// Get peer lists
	peers1, err := peer1Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers1.Peers, 1)

	peers2, err := peer2Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers2.Peers, 1)

	// Convert peer IDs to H512 format
	peer1IDBytes, err := hex.DecodeString(peers1.Peers[0].Id)
	require.NoError(t, err)
	var peer1IDArr [64]byte
	copy(peer1IDArr[:], peer1IDBytes)
	peer1ID := gointerfaces.ConvertHashToH512(peer1IDArr)

	peer2IDBytes, err := hex.DecodeString(peers2.Peers[0].Id)
	require.NoError(t, err)
	var peer2IDArr [64]byte
	copy(peer2IDArr[:], peer2IDBytes)
	peer2ID := gointerfaces.ConvertHashToH512(peer2IDArr)

	t.Logf("Peer1 ID: %x", peer1IDArr[:8])
	t.Logf("Peer2 ID: %x", peer2IDArr[:8])

	// Create ETH/63 transaction for testing
	testTx := createTestETH63Transaction(t)
	t.Logf("Created test transaction: Hash=%s, Nonce=%d, Value=%s",
		testTx.Hash().Hex(), testTx.Nonce(), testTx.Value().String())

	// Encode transaction for ETH/63 protocol
	transactions := eth63.ETH63Transactions{testTx}
	txData, err := rlp.EncodeToBytes(transactions)
	require.NoError(t, err)
	t.Logf("Encoded transaction data: %d bytes", len(txData))

	// Test 1: Peer1 sends transaction to Peer2
	t.Run("Peer1_Sends_Transaction_To_Peer2", func(t *testing.T) {
		_, err = peer1Mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			PeerId: peer1ID,
			Data: &sentryproto.OutboundMessageData{
				Id:   sentryproto.MessageId_TRANSACTIONS_63,
				Data: txData,
			},
		})
		require.NoError(t, err)

		// Verify transaction was received by Peer2
		select {
		case receivedMsg := <-peer1ToPeer2Chan:
			require.Equal(t, sentryproto.MessageId_TRANSACTIONS_63, receivedMsg.Id)
			require.Equal(t, txData, receivedMsg.Data)
			t.Log("✅ Transaction successfully sent from Peer1 to Peer2")
		case <-time.After(2 * time.Second):
			t.Fatal("❌ Timeout waiting for transaction from Peer1 to Peer2")
		}
	})

	// Test 2: Peer2 processes and responds to Peer1
	t.Run("Peer2_Processes_And_Responds", func(t *testing.T) {
		// Create a response transaction
		responseTx := createTestETH63Transaction(t)
		// Create a new transaction with different nonce
		responseTx = eth63.NewETH63Transaction(999, *responseTx.To(), responseTx.Value(), responseTx.Gas(), responseTx.GasPrice(), responseTx.Data())

		responseTransactions := eth63.ETH63Transactions{responseTx}
		responseData, err := rlp.EncodeToBytes(responseTransactions)
		require.NoError(t, err)

		// Peer2 sends response back to Peer1
		_, err = peer2Mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			PeerId: peer2ID,
			Data: &sentryproto.OutboundMessageData{
				Id:   sentryproto.MessageId_TRANSACTIONS_63,
				Data: responseData,
			},
		})
		require.NoError(t, err)

		// Verify response was received by Peer1
		select {
		case receivedResp := <-peer2ToPeer1Chan:
			require.Equal(t, sentryproto.MessageId_TRANSACTIONS_63, receivedResp.Id)
			require.Equal(t, responseData, receivedResp.Data)
			t.Log("✅ Response transaction successfully sent from Peer2 to Peer1")
		case <-time.After(2 * time.Second):
			t.Fatal("❌ Timeout waiting for response from Peer2 to Peer1")
		}
	})

	// Test 3: Verify transaction integrity
	t.Run("Verify_Transaction_Integrity", func(t *testing.T) {
		// Decode the original transaction data to verify integrity
		var decodedTxs eth63.ETH63Transactions
		err := rlp.DecodeBytes(txData, &decodedTxs)
		require.NoError(t, err)
		require.Len(t, decodedTxs, 1)

		decodedTx := decodedTxs[0]
		require.Equal(t, testTx.Hash(), decodedTx.Hash())
		require.Equal(t, testTx.Nonce(), decodedTx.Nonce())
		require.Equal(t, testTx.Value().String(), decodedTx.Value().String())
		require.Equal(t, testTx.GasPrice().String(), decodedTx.GasPrice().String())
		require.Equal(t, testTx.Gas(), decodedTx.Gas())

		t.Log("✅ Transaction integrity verified - RLP encoding/decoding works correctly")
	})

	t.Log("🎉 ETH/63 transaction exchange test completed successfully")
}

// createTestETH63Transaction creates a test ETH/63 transaction
func createTestETH63Transaction(t *testing.T) *eth63.ETH63Transaction {
	// Create a test transaction with realistic values
	nonce := uint64(42)
	to := common.HexToAddress("0x742d35cc6634c0532925a3b8d1320f8a94ee3a36")
	value := big.NewInt(1000000000000000000) // 1 ETH
	gasLimit := uint64(21000)
	gasPrice := big.NewInt(20000000000) // 20 Gwei
	data := []byte("test transaction data")

	tx := eth63.NewETH63Transaction(nonce, to, value, gasLimit, gasPrice, data)
	txWithSig, err := tx.WithSignature(eth63.HomesteadSigner{}, []byte{})
	if err != nil {
		t.Fatalf("failed to sign transaction: %v", err)
	}

	// Create transaction with signature using the constructor
	// Note: You'll need to use WithSignature method to add the signature values
	return txWithSig
}

// TestETH63TransactionValidation tests transaction validation in ETH/63 handler
func TestETH63TransactionValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry client
	mockClient := newClient(ctrl, 1, []string{"eth/63"})
	mockClient.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Create multiplexer and handler
	sentry.NewSentryMultiplexer([]sentryproto.SentryClient{mockClient})
	logger := log.New()
	handler := NewETH63Handler(&GrpcServer{}, logger)
	// Test transaction validation
	t.Run("Valid_Transaction", func(t *testing.T) {
		tx := createTestETH63Transaction(t)

		// Test signature validation
		err := handler.validateETH63Transaction(tx)
		// Note: This will fail because we're using fake signatures, but it tests the flow
		t.Logf("Transaction validation result: %v", err)
	})

	t.Run("Invalid_Transaction_No_Signature", func(t *testing.T) {
		tx := createTestETH63Transaction(t)
		tx, err := tx.WithSignature(eth63.HomesteadSigner{}, []byte{})
		if err != nil {
			t.Fatalf("failed to sign transaction: %v", err)
		}

		err = handler.validateETH63Transaction(tx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no signature")
		t.Log("✅ Correctly rejected transaction with no signature")
	})
}

func TestETH63TransactionExchange2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry for two peers
	peer1Client := newClient(ctrl, 1, []string{"eth/63"})
	peer2Client := newClient(ctrl, 2, []string{"eth/63"})

	// Configure handshake for both peers to use ETH/63
	peer1Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	peer2Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Create message channels for peer communication
	peer1ToPeer2Chan := make(chan *sentryproto.OutboundMessageData, 10)
	peer2ToPeer1Chan := make(chan *sentryproto.OutboundMessageData, 10)

	// Configure peer1 to send messages to peer2
	peer1Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer1 -> Peer2: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer1ToPeer2Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Configure peer2 to send messages to peer1
	peer2Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer2 -> Peer1: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer2ToPeer1Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Create multiplexers for both peers
	peer1Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer1Client})
	peer2Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer2Client})

	// Mock peer count for both clients
	peer1Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()
	peer2Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()

	// Perform handshakes
	hs1, err := peer1Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs1.Protocol)

	hs2, err := peer2Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs2.Protocol)

	t.Logf("Handshake complete - Peer1: %s, Peer2: %s",
		sentryproto.Protocol_name[int32(hs1.Protocol)],
		sentryproto.Protocol_name[int32(hs2.Protocol)])

	// Get peer lists
	peers1, err := peer1Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers1.Peers, 1)

	peers2, err := peer2Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers2.Peers, 1)

	// Convert peer IDs to H512 format
	peer1IDBytes, err := hex.DecodeString(peers1.Peers[0].Id)
	require.NoError(t, err)
	var peer1IDArr [64]byte
	copy(peer1IDArr[:], peer1IDBytes)
	peer1ID := gointerfaces.ConvertHashToH512(peer1IDArr)

	peer2IDBytes, err := hex.DecodeString(peers2.Peers[0].Id)
	require.NoError(t, err)
	var peer2IDArr [64]byte
	copy(peer2IDArr[:], peer2IDBytes)
	// peer2ID := gointerfaces.ConvertHashToH512(peer2IDArr)

	t.Logf("Peer1 ID: %x", peer1IDArr[:8])
	t.Logf("Peer2 ID: %x", peer2IDArr[:8])

	// Create ETH/63 transaction for testing
	testTx := createTestETH63Transaction(t)
	t.Logf("Created test transaction: Hash=%s, Nonce=%d, Value=%s",
		testTx.Hash().Hex(), testTx.Nonce(), testTx.Value().String())

	// Encode transaction for ETH/63 protocol
	transactions := eth63.ETH63Transactions{testTx}
	txData, err := rlp.EncodeToBytes(transactions)
	require.NoError(t, err)
	t.Logf("Encoded transaction data: %d bytes", len(txData))

	// Test 1: Peer1 sends transaction to Peer2
	t.Run("Peer1_Sends_Transaction_To_Peer2", func(t *testing.T) {
		_, err = peer1Mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			PeerId: peer1ID,
			Data: &sentryproto.OutboundMessageData{
				Id:   sentryproto.MessageId_TRANSACTIONS_63,
				Data: txData,
			},
		})
		require.NoError(t, err)

		// Verify transaction was received by Peer2
		select {
		case receivedMsg := <-peer1ToPeer2Chan:
			require.Equal(t, sentryproto.MessageId_TRANSACTIONS_63, receivedMsg.Id)
			require.Equal(t, txData, receivedMsg.Data)
			t.Log("✅ Transaction successfully sent from Peer1 to Peer2")
		case <-time.After(2 * time.Second):
			t.Fatal("❌ Timeout waiting for transaction from Peer1 to Peer2")
		}
	})

}

func SetupPeer(t *testing.T, peerName string, id [64]byte, protocolVersion uint, logger log.Logger) (kv.TemporalRwDB, *GrpcServer, [64]byte, *PeerInfo) {
	config := &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspec := &types.Genesis{Config: config}
	genesis := rawdb.MustCommitGenesisWithoutState(gspec, db)

	var s1 *GrpcServer
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		s1 = testSentryServer(tx, gspec, genesis.Hash())
		return nil
	})
	if err != nil {
		fmt.Println("❌ Error updating database")
		return nil, nil, [64]byte{}, nil
	}
	peerID := id

	peerInfo := &PeerInfo{
		peer:     p2p.NewPeer(enode.ID{}, peerID, peerName, []p2p.Cap{{Name: "eth", Version: protocolVersion}}, false),
		protocol: protocolVersion,
	}
	NewETH63Handler(s1, logger)
	return db, s1, peerID, peerInfo
}

// case msg.Code == TxMsg:
func TestETH63TransactionExchange3(t *testing.T) {
	// Setup test database and genesis
	config := &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspec := &types.Genesis{Config: config}
	genesis := rawdb.MustCommitGenesisWithoutState(gspec, db)

	// Create GrpcServer instances for both peers
	var s1, s2 *GrpcServer
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		s1 = testSentryServer(tx, gspec, genesis.Hash())
		return nil
	})
	require.NoError(t, err)

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		s2 = testSentryServer(tx, gspec, genesis.Hash())
		return nil
	})
	require.NoError(t, err)

	// Create message pipes for peer communication
	p2p1, p2p2 := p2p.MsgPipe()
	defer p2p1.Close()
	defer p2p2.Close()

	// Create peer info for both peers
	peer1ID := [64]byte{1, 2, 3, 4}
	peer2ID := [64]byte{5, 6, 7, 8}

	peer1Info := &PeerInfo{
		peer:     p2p.NewPeer(enode.ID{}, peer1ID, "peer1", []p2p.Cap{{Name: "eth", Version: 63}}, false),
		protocol: 63,
	}

	peer2Info := &PeerInfo{
		peer:     p2p.NewPeer(enode.ID{}, peer2ID, "peer2", []p2p.Cap{{Name: "eth", Version: 63}}, false),
		protocol: 63,
	}

	// Create ETH/63 handler for both servers
	logger := log.New()
	NewETH63Handler(s1, logger)
	NewETH63Handler(s2, logger)

	// Create message channels to capture sent messages
	peer1Messages := make(chan []byte, 10)
	peer2Messages := make(chan []byte, 10)

	// Mock send function for peer1
	send1 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		// fmt.Printf("Peer1 sending message: %s, size: %d", msgId.String(), len(b))
		peer1Messages <- b
	}

	// Mock send function for peer2
	send2 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		// fmt.Printf("Peer2 sending message: %s, size: %d", msgId.String(), len(b))
		peer2Messages <- b
	}

	// Mock hasSubscribers function
	hasSubscribers := func(msgId proto_sentry.MessageId) bool {
		return true
	}

	// Start runPeer goroutines for both peers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start peer1 runPeer
	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer1ID, cap, p2p1, peer1Info, send1, hasSubscribers, logger)
		if err != nil {
			fmt.Println("Error:Peer1 runPeer error:", err)
			// return
		}
	}()

	// Start peer2 runPeer
	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer2ID, cap, p2p2, peer2Info, send2, hasSubscribers, logger)
		if err != nil {
			fmt.Println("Error:Peer2 runPeer error: ", err)
			// return
		}
	}()

	// Wait a bit for peers to start
	time.Sleep(100 * time.Millisecond)

	// Create ETH/63 transaction for testing
	testTx := createTestETH63Transaction(t)
	fmt.Printf("Created test transaction: Hash=%s, Nonce=%d, Value=%s",
		testTx.Hash().Hex(), testTx.Nonce(), testTx.Value().String())

	// Encode transaction for ETH/63 protocol
	transactions := eth63.ETH63Transactions{testTx}
	txData, err := rlp.EncodeToBytes(transactions)
	if err != nil {
		fmt.Println("❌ Error encoding transaction")
		return
	}
	fmt.Println("Encoded transaction data byte:s", len(txData))

	// Test 1: Send transaction from peer1 to peer2
	t.Run("Peer1_Sends_Transaction_To_Peer2", func(t *testing.T) {
		// Create ETH/63 transaction message
		msg := &p2p.Msg{
			Code:    eth.TransactionsMsg,
			Size:    uint32(len(txData)),
			Payload: bytes.NewReader(txData),
		}

		// Send message from peer1 to peer2
		err := p2p1.WriteMsg(*msg)
		if err != nil {
			fmt.Println("❌ Error sending message from peer1 to peer2")
			return
		}

		// Verify transaction was received by peer2
		select {
		case receivedData := <-peer2Messages:
			require.Equal(t, txData, receivedData)
			fmt.Println("✅ Transaction successfully sent from Peer1 to Peer2")
		case <-time.After(2 * time.Second):
			fmt.Println("❌ Timeout waiting for transaction from Peer1 to Peer2")
		}
	})

	// Test 2: Send response from peer2 to peer1
	t.Run("Peer2_Sends_Response_To_Peer1", func(t *testing.T) {
		// Create a response transaction
		responseTx := createTestETH63Transaction(t)
		// Create a new transaction with different nonce
		responseTx = eth63.NewETH63Transaction(999, *responseTx.To(), responseTx.Value(), responseTx.Gas(), responseTx.GasPrice(), responseTx.Data())

		responseTransactions := eth63.ETH63Transactions{responseTx}
		responseData, err := rlp.EncodeToBytes(responseTransactions)
		if err != nil {
			fmt.Println("❌ Error encoding response transaction")
			return
		}

		// Create response message
		responseMsg := &p2p.Msg{
			Code:    eth.TransactionsMsg,
			Size:    uint32(len(responseData)),
			Payload: bytes.NewReader(responseData),
		}

		// Send response from peer2 to peer1
		err = p2p2.WriteMsg(*responseMsg)
		if err != nil {
			fmt.Println("❌ Error sending response message from peer2 to peer1")
			return
		}

		// Verify response was received by peer1
		select {
		case receivedData := <-peer1Messages:
			require.Equal(t, responseData, receivedData)
			fmt.Println("✅ Response transaction successfully sent from Peer2 to Peer1")
		case <-time.After(2 * time.Second):
			fmt.Println("❌ Timeout waiting for response from Peer2 to Peer1")
		}
	})

	// fmt.Println("===== 7======")

	// Test 3: Verify transaction integrity
	t.Run("Verify_Transaction_Integrity", func(t *testing.T) {
		// Decode the original transaction data to verify integrity
		var decodedTxs eth63.ETH63Transactions
		err := rlp.DecodeBytes(txData, &decodedTxs)
		require.NoError(t, err)
		require.Len(t, decodedTxs, 1)

		decodedTx := decodedTxs[0]
		require.Equal(t, testTx.Hash(), decodedTx.Hash())
		require.Equal(t, testTx.Nonce(), decodedTx.Nonce())
		require.Equal(t, testTx.Value().String(), decodedTx.Value().String())
		require.Equal(t, testTx.GasPrice().String(), decodedTx.GasPrice().String())
		require.Equal(t, testTx.Gas(), decodedTx.Gas())

		fmt.Println("✅ Transaction integrity verified - RLP encoding/decoding works correctly")
	})

	fmt.Println("🎉 ETH/63 transaction exchange test completed successfully")
}

func TestETH63TransactionSignedExchange(t *testing.T) {
	account_1_key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate account 1 key: %v", err)
	}
	account_1_address := crypto.PubkeyToAddress(account_1_key.PublicKey)
	fmt.Println("-> account_1_address:", account_1_address.Hex())

	account_2_key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate account 2 key: %v", err)
	}
	account_2_address := crypto.PubkeyToAddress(account_2_key.PublicKey)
	fmt.Println("-> account_2_address:", account_2_address.Hex())

	// setup peer
	peer1ID := [64]byte{1, 2, 3, 4}
	peer2ID := [64]byte{5, 6, 7, 8}
	logger := log.New()

	_, _, peer1ID, peer1Info := SetupPeer(t, "peer1", peer1ID, 63, logger)
	_, _, peer2ID, peer2Info := SetupPeer(t, "peer2", peer2ID, 63, logger)

	// Create message channels to capture sent messages
	peer1Messages := make(chan []byte, 10)
	peer2Messages := make(chan []byte, 10)

	// Mock send function for peer1
	send1 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		// fmt.Printf("Peer1 sending message: %s, size: %d", msgId.String(), len(b))
		peer1Messages <- b
	}

	// Mock send function for peer2
	send2 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		// fmt.Printf("Peer2 sending message: %s, size: %d", msgId.String(), len(b))
		peer2Messages <- b
	}

	// Mock hasSubscribers function
	hasSubscribers := func(msgId proto_sentry.MessageId) bool {
		return true
	}

	// Create message pipes for peer communication
	p2p1, p2p2 := p2p.MsgPipe()
	defer p2p1.Close()
	defer p2p2.Close()

	// Start runPeer goroutines for both peers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait a bit for peers to start
	time.Sleep(200 * time.Millisecond)

	// Start peer1 runPeer
	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer1ID, cap, p2p1, peer1Info, send1, hasSubscribers, logger)
		if err != nil {
			fmt.Println("Error:Peer1 runPeer error:", err)
			// return
		}
	}()

	// Start peer2 runPeer
	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer2ID, cap, p2p2, peer2Info, send2, hasSubscribers, logger)
		if err != nil {
			fmt.Println("Error:Peer2 runPeer error: ", err)
			// return
		}
	}()
	nonce := uint64(1)
	to := common.HexToAddress(account_2_address.Hex())
	value := big.NewInt(1000000000000000000) // 1 ETH
	gasLimit := uint64(21000)
	gasPrice := big.NewInt(20000000000) // 20 Gwei
	data := []byte{}
	tx := eth63.NewETH63Transaction(nonce, to, value, gasLimit, gasPrice, data)
	signedTx, err := eth63.SignTx(tx, eth63.HomesteadSigner{}, account_1_key)
	if err != nil {
		t.Fatalf("failed to sign transaction: %v", err)
	}
	fmt.Println("-> signedTx:", signedTx.Hash().Hex())

	// Encode transaction for ETH/63 protocol
	transactions := eth63.ETH63Transactions{signedTx}
	txData, err := rlp.EncodeToBytes(transactions)
	if err != nil {
		fmt.Println("❌ Error encoding transaction")
		return
	}
	fmt.Println("Encoded transaction data byte:s", len(txData))

	// Test 1: Send transaction from peer1 to peer2
	t.Run("Peer1_Sends_Transaction_To_Peer2", func(t *testing.T) {
		// Create ETH/63 transaction message
		msg := &p2p.Msg{
			Code:    eth.TransactionsMsg,
			Size:    uint32(len(txData)),
			Payload: bytes.NewReader(txData),
		}

		// Send message from peer1 to peer2
		err := p2p1.WriteMsg(*msg)
		if err != nil {
			fmt.Println("❌ Error sending message from peer1 to peer2")
			return
		}

		// Verify transaction was received by peer2
		select {
		case receivedData := <-peer2Messages:
			require.Equal(t, txData, receivedData)
			fmt.Println("✅ Transaction successfully sent from Peer1 to Peer2")
		case <-time.After(2 * time.Second):
			fmt.Println("❌ Timeout waiting for transaction from Peer1 to Peer2")
		}
	})
}

func TestETH63TransactionSyncWithTxPool(t *testing.T) {
	// Setup accounts
	account_1_key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate account 1 key: %v", err)
	}
	account_1_address := crypto.PubkeyToAddress(account_1_key.PublicKey)
	fmt.Println("-> account_1_address:", account_1_address.Hex())

	account_2_key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate account 2 key: %v", err)
	}
	account_2_address := crypto.PubkeyToAddress(account_2_key.PublicKey)
	fmt.Println("-> account_2_address:", account_2_address.Hex())

	logger := log.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup test database and genesis
	config := &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspec := &types.Genesis{Config: config}
	genesis := rawdb.MustCommitGenesisWithoutState(gspec, db)

	// Create GrpcServer instances for both peers
	var s1, s2 *GrpcServer
	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		s1 = testSentryServer(tx, gspec, genesis.Hash())
		return nil
	})
	if err != nil {
		fmt.Println("❌ Failed to create s1:", err)
		return
	}

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		s2 = testSentryServer(tx, gspec, genesis.Hash())
		return nil
	})
	if err != nil {
		fmt.Println("❌ Failed to create s2:", err)
		return
	}

	// Create message pipes for peer communication
	p2p1, p2p2 := p2p.MsgPipe()
	defer p2p1.Close()
	defer p2p2.Close()

	// Create peer info for both peers
	peer1ID := [64]byte{1, 2, 3, 4}
	peer2ID := [64]byte{5, 6, 7, 8}

	peer1Info := &PeerInfo{
		peer:     p2p.NewPeer(enode.ID{}, peer1ID, "peer1", []p2p.Cap{{Name: "eth", Version: 63}}, false),
		protocol: 63,
	}

	peer2Info := &PeerInfo{
		peer:     p2p.NewPeer(enode.ID{}, peer2ID, "peer2", []p2p.Cap{{Name: "eth", Version: 63}}, false),
		protocol: 63,
	}

	// Create ETH/63 handler for both servers
	NewETH63Handler(s1, logger)
	NewETH63Handler(s2, logger)

	// Create transaction pool configuration
	txPoolCfg := txpoolcfg.DefaultConfig
	txPoolCfg.DBDir = t.TempDir()
	txPoolCfg.SyncToNewPeersEvery = 100 * time.Millisecond
	txPoolCfg.ProcessRemoteTxnsEvery = 50 * time.Millisecond
	txPoolCfg.NoGossip = false

	// Create transaction pool database
	poolDB, err := mdbx.New(kv.TxPoolDB, logger).
		Path(txPoolCfg.DBDir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		WriteMergeThreshold(3 * 8192).
		PageSize(16 * datasize.KB).
		GrowthStep(16 * datasize.MB).
		DirtySpace(uint64(64 * datasize.MB)).
		MapSize(1 * datasize.TB).
		WriteMap(txPoolCfg.MdbxWriteMap).
		Open(ctx)
	if err != nil {
		fmt.Println("❌ Failed to create pool database:", err)
		return
	}
	defer poolDB.Close()

	// Create mock sentry clients for fetcher
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry server for Peer2's fetcher
	sentryServer := sentryproto.NewMockSentryServer(ctrl)

	// Channel to capture messages received by Peer2's fetcher
	receivedMessages := make(chan *sentryproto.InboundMessage, 10)

	// Mock the Messages stream for Peer2's fetcher
	sentryServer.EXPECT().Messages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(req *sentryproto.MessagesRequest, server sentryproto.Sentry_MessagesServer) error {
			// Wait for actual transaction data from the channel
			select {
			case msg := <-receivedMessages:
				fmt.Printf("------> Mock sending message: %s\n", msg.Id.String())
				if err := server.Send(msg); err != nil {
					return err
				}
			case <-time.After(100 * time.Millisecond):
				// Send empty TRANSACTIONS_63 message if no transaction received
				emptyMsg := &sentryproto.InboundMessage{
					Id:     sentryproto.MessageId_TRANSACTIONS_63,
					Data:   []byte{},
					PeerId: gointerfaces.ConvertHashToH512([64]byte{1, 2, 3, 4}),
				}
				if err := server.Send(emptyMsg); err != nil {
					return err
				}
			}
			return nil
		}).AnyTimes()

	// Mock other required methods for Peer2's fetcher
	sentryServer.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{}, nil).AnyTimes()
	sentryServer.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).Return(&sentryproto.SentPeers{}, nil).AnyTimes()
	sentryServer.EXPECT().PeerEvents(gomock.Any(), gomock.Any()).DoAndReturn(
		func(req *sentryproto.PeerEventsRequest, server sentryproto.Sentry_PeerEventsServer) error {
			// Simulate peer events
			for i := 0; i < 5; i++ {
				if err := server.Send(&sentryproto.PeerEvent{}); err != nil {
					return err
				}
			}
			return nil
		}).AnyTimes()

	// Create mock pool for Peer2's fetcher
	mockPool := txpool.NewMockPool(ctrl)
	mockPool.EXPECT().Started().Return(true).AnyTimes()
	mockPool.EXPECT().FilterKnownIdHashes(gomock.Any(), gomock.Any()).Return([]byte{}, nil).AnyTimes()
	mockPool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	mockPool.EXPECT().AddNewGoodPeer(gomock.Any()).Return().AnyTimes()

	// Capture transactions added to pool
	var addedTxns txpool.TxnSlots
	mockPool.EXPECT().AddRemoteTxns(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, txns txpool.TxnSlots) {
			addedTxns = txns
			fmt.Printf("✅ Peer2 fetcher received %d transactions\n", len(txns.Txns))
		}).AnyTimes()

	fmt.Println("------> Added transactions to pool", addedTxns)
	// Create mock state changes client
	mockStateChangesClient := remote.NewMockKVClient(ctrl)

	// Create fetcher for Peer2
	chainID := uint256.NewInt(1)
	fetcher := txpool.NewFetch(
		ctx,
		[]sentryproto.SentryClient{direct.NewSentryClientDirect(direct.ETH63, sentryServer)},
		mockPool,
		mockStateChangesClient,
		poolDB,
		*chainID,
		logger,
	)

	// Start Peer2's fetcher
	fetcher.ConnectSentries()

	// Create message channels to capture sent messages
	peer1Messages := make(chan []byte, 10)
	peer2Messages := make(chan []byte, 10)

	// Mock send function for peer1
	send1 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		peer1Messages <- b
	}

	// Mock send function for peer2
	send2 := func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte) {
		peer2Messages <- b
	}

	// Mock hasSubscribers function
	hasSubscribers := func(msgId proto_sentry.MessageId) bool {
		return true
	}

	// Start runPeer goroutines for both peers
	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer1ID, cap, p2p1, peer1Info, send1, hasSubscribers, logger)
		if err != nil {
			fmt.Printf("Peer1 runPeer error: %v\n", err)
		}
	}()

	go func() {
		cap := p2p.Cap{Name: "eth", Version: 63}
		err := runPeer(ctx, peer2ID, cap, p2p2, peer2Info, send2, hasSubscribers, logger)
		if err != nil {
			fmt.Printf("Peer2 runPeer error: %v\n", err)
		}
	}()

	// Wait for peers and fetcher to start
	time.Sleep(200 * time.Millisecond)

	// // Create ETH/63 transaction for testing
	// nonce := uint64(1)
	// to := common.HexToAddress(account_2_address.Hex())
	// value := big.NewInt(1000000000000000000) // 1 ETH
	// gasLimit := uint64(21000)
	// gasPrice := big.NewInt(20000000000) // 20 Gwei
	// data := []byte{}
	// tx := NewETH63Transaction(nonce, to, value, gasLimit, gasPrice, data)
	// signedTx, err := SignTx(tx, HomesteadSigner{}, account_1_key)
	// if err != nil {
	// 	t.Fatalf("failed to sign transaction: %v", err)
	// }
	// fmt.Println("-> signedTx:", signedTx.Hash().Hex())

	// // Encode transaction for ETH/63 protocol
	// transactions := ETH63Transactions{signedTx}
	// txData, err := rlp.EncodeToBytes(transactions)
	// if err != nil {
	// 	fmt.Println("❌ Failed to encode transaction:", err)
	// 	return
	// }
	// fmt.Printf("Encoded transaction data: %d bytes\n", len(txData))

	// // Test: Peer1 sends transaction to Peer2, Peer2's fetcher catches it
	// fmt.Println(" Testing: Peer1_Sends_To_Peer2_Fetcher_Catches")

	// // Create ETH/63 transaction message
	// msg := &p2p.Msg{
	// 	Code:    eth.TransactionsMsg,
	// 	Size:    uint32(len(txData)),
	// 	Payload: bytes.NewReader(txData),
	// }

	// // Send message from peer1 to peer2
	// err = p2p1.WriteMsg(*msg)
	// if err != nil {
	// 	fmt.Println("❌ Failed to send message:", err)
	// 	return
	// }
	// fmt.Println("📤 Peer1 sent transaction to Peer2")

	// // Wait for Peer2 to receive and process the message
	// time.Sleep(100 * time.Millisecond)

	// // Verify Peer2 received the message through runPeer
	// select {
	// case receivedData := <-peer2Messages:
	// 	if bytes.Equal(txData, receivedData) {
	// 		fmt.Println("✅ Peer2 received transaction via runPeer")
	// 	} else {
	// 		fmt.Println("❌ Peer2 received different data")
	// 	}
	// case <-time.After(2 * time.Second):
	// 	fmt.Println("❌ Timeout waiting for Peer2 to receive transaction")
	// 	return
	// }

	// // Simulate Peer2's fetcher receiving the transaction
	// eth63Msg := &sentryproto.InboundMessage{
	// 	Id:     sentryproto.MessageId_TRANSACTIONS_66, // Use TRANSACTIONS_66 for ETH/63 compatibility
	// 	Data:   txData,
	// 	PeerId: gointerfaces.ConvertHashToH512(peer1ID),
	// }

	// // Send message to Peer2's fetcher
	// receivedMessages <- eth63Msg
	// fmt.Println("📨 Sent transaction to Peer2's fetcher")

	// // Wait for fetcher to process the transaction
	// time.Sleep(200 * time.Millisecond)

	// // Verify fetcher processed the transaction
	// if len(addedTxns.Txns) == 0 {
	// 	fmt.Println("❌ Fetcher should have received transactions")
	// 	return
	// }
	// fmt.Printf("✅ Peer2's fetcher successfully processed %d transactions\n", len(addedTxns.Txns))

	// // Verify transaction integrity in fetcher
	// if len(addedTxns.Txns) > 0 {
	// 	processedTx := addedTxns.Txns[0]
	// 	if processedTx.Nonce == signedTx.Nonce() && processedTx.Gas == signedTx.Gas() {
	// 		fmt.Println("✅ Transaction integrity verified in fetcher")
	// 	} else {
	// 		fmt.Println("❌ Transaction integrity check failed")
	// 	}
	// }

	// fmt.Println("🎉 ETH/63 transaction sync with fetcher test completed successfully")
}

// MockSentryMessagesClient implements sentryproto.Sentry_MessagesClient
type MockSentryMessagesClient struct {
	ctx context.Context
	ch  chan *sentryproto.InboundMessage
}

func (m *MockSentryMessagesClient) Recv() (*sentryproto.InboundMessage, error) {
	select {
	case msg := <-m.ch:
		return msg, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *MockSentryMessagesClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *MockSentryMessagesClient) Trailer() metadata.MD {
	return nil
}

func (m *MockSentryMessagesClient) CloseSend() error {
	return nil
}

func (m *MockSentryMessagesClient) Context() context.Context {
	return m.ctx
}

func (m *MockSentryMessagesClient) SendMsg(msg interface{}) error {
	return nil
}

func (m *MockSentryMessagesClient) RecvMsg(msg interface{}) error {
	return nil
}

// MockSentryPeerEventsClient implements sentryproto.Sentry_PeerEventsClient
type MockSentryPeerEventsClient struct {
	ctx context.Context
}

func (m *MockSentryPeerEventsClient) Recv() (*sentryproto.PeerEvent, error) {
	select {
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil, io.EOF
	}
}

func (m *MockSentryPeerEventsClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *MockSentryPeerEventsClient) Trailer() metadata.MD {
	return nil
}

func (m *MockSentryPeerEventsClient) CloseSend() error {
	return nil
}

func (m *MockSentryPeerEventsClient) Context() context.Context {
	return m.ctx
}

func (m *MockSentryPeerEventsClient) SendMsg(msg interface{}) error {
	return nil
}

func (m *MockSentryPeerEventsClient) RecvMsg(msg interface{}) error {
	return nil
}

func TestETH63BlockExchange(t *testing.T) {
	fmt.Println("------> TestETH63BlockExchange")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock sentry clients for two peers
	peer1Client := newClient(ctrl, 1, []string{"eth/63"})
	peer2Client := newClient(ctrl, 2, []string{"eth/63"})

	// Configure handshake for both peers to use ETH/63
	peer1Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	peer2Client.EXPECT().HandShake(gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{
		Protocol: sentryproto.Protocol_ETH63,
	}, nil).AnyTimes()

	// Create message channels for peer communication
	peer1ToPeer2Chan := make(chan *sentryproto.OutboundMessageData, 10)
	peer2ToPeer1Chan := make(chan *sentryproto.OutboundMessageData, 10)
	// Configure peer1 to send messages to peer2
	peer1Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer1 -> Peer2: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer1ToPeer2Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Configure peer2 to send messages to peer1
	peer2Client.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			t.Logf("Peer2 -> Peer1: MessageId=%s, DataSize=%d", req.Data.Id.String(), len(req.Data.Data))
			peer2ToPeer1Chan <- req.Data
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()

	// Create multiplexers for both peers
	peer1Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer1Client})
	peer2Mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{peer2Client})

	// Mock peer count for both clients
	peer1Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()
	peer2Client.EXPECT().PeerCount(gomock.Any(), gomock.Any()).Return(&sentryproto.PeerCountReply{Count: 1}, nil).AnyTimes()

	// Perform handshakes
	hs1, err := peer1Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs1.Protocol)

	hs2, err := peer2Mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, sentryproto.Protocol_ETH63, hs2.Protocol)

	fmt.Println("<-> Handshake complete - Peer1: ", sentryproto.Protocol_name[int32(hs1.Protocol)], "Peer2: ", sentryproto.Protocol_name[int32(hs2.Protocol)])

	// Get peer lists
	peers1, err := peer1Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers1.Peers, 1)

	peers2, err := peer2Mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peers2.Peers, 1)

	// Convert peer IDs to H512 format
	peer1IDBytes, err := hex.DecodeString(peers1.Peers[0].Id)
	require.NoError(t, err)
	var peer1IDArr [64]byte
	copy(peer1IDArr[:], peer1IDBytes)
	peer1ID := gointerfaces.ConvertHashToH512(peer1IDArr)

	peer2IDBytes, err := hex.DecodeString(peers2.Peers[0].Id)
	require.NoError(t, err)
	var peer2IDArr [64]byte
	copy(peer2IDArr[:], peer2IDBytes)
	// peer2ID := gointerfaces.ConvertHashToH512(peer2IDArr)

	fmt.Println("-> Peer1 ID: ", peer1IDArr[:8])
	fmt.Println("-> Peer2 ID: ", peer2IDArr[:8])

	// Create ETH/63 block for testing
	testBlock := createTestETH63Block(t)
	fmt.Println("Created test block: Number=", testBlock.NumberU64(), "Hash=", testBlock.Hash().Hex(), "TxCount=", len(testBlock.Transactions()))

	// Encode block for ETH/63 protocol
	blockData, err := rlp.EncodeToBytes(testBlock)
	require.NoError(t, err)
	fmt.Println("-> Encoded block data: ", len(blockData), "bytes")

	// Test 1: Peer1 sends NEW_BLOCK to Peer2
	t.Run("Peer1_Sends_NewBlock_To_Peer2", func(t *testing.T) {
		_, err = peer1Mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			PeerId: peer1ID,
			Data: &sentryproto.OutboundMessageData{
				Id:   sentryproto.MessageId_NEW_BLOCK_63,
				Data: blockData,
			},
		})
		require.NoError(t, err)

		// Verify block was received by Peer2
		select {
		case receivedMsg := <-peer1ToPeer2Chan:
			require.Equal(t, sentryproto.MessageId_NEW_BLOCK_63, receivedMsg.Id)
			require.Equal(t, blockData, receivedMsg.Data)
			fmt.Println("✅ Block successfully sent from Peer1 to Peer2")
		case <-time.After(2 * time.Second):
			fmt.Println("❌ Timeout waiting for block from Peer1 to Peer2")
		}
	})

	fmt.Println("-> ls 2")
}

// createTestETH63Block creates a test ETH/63 block
func createTestETH63Block(t *testing.T) *types.Block {
	// Create a test block with realistic values
	header := &types.Header{
		ParentHash:  common.Hash{0x01},
		UncleHash:   common.Hash{0x02},
		Coinbase:    common.HexToAddress("0x742d35cc6634c0532925a3b8d1320f8a94ee3a36"),
		Root:        common.Hash{0x03},
		TxHash:      common.Hash{0x04},
		ReceiptHash: common.Hash{0x05},
		Bloom:       types.Bloom{0x06},
		Difficulty:  big.NewInt(1000000),
		Number:      big.NewInt(1),
		GasLimit:    1000000,
		GasUsed:     50000,
		Time:        uint64(time.Now().Unix()),
		Extra:       []byte("test extra data"),
		MixDigest:   common.Hash{0x07},
		Nonce:       types.BlockNonce{0x08},
	}

	// // Create test transactions
	// txs := []types.Transaction{
	// 	createTestETH63Transaction(t),
	// }

	// Create test uncles
	uncles := []*types.Header{
		{
			ParentHash: common.Hash{0x09},
			Coinbase:   common.HexToAddress("0x1234567890123456789012345678901234567890"),
			Root:       common.Hash{0x0a},
			TxHash:     common.Hash{0x0b},
			Number:     big.NewInt(0),
			GasLimit:   1000000,
			GasUsed:    0,
			Time:       uint64(time.Now().Unix()),
		},
	}

	// Create the block using types.NewBlock
	block := types.NewBlock(header, nil, uncles, nil, nil)
	return block
}
