package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

// TCPPeer represents the remote node over a TCP established connection.
type TCPPeer struct {
	// The underlying connection of the peer
	// in this case is TCP connection
	net.Conn
	// if we dial and retrive a conn => outbound == true
	// if we accept and retrive a conn => outbound == false
	outbound bool

	Wg *sync.WaitGroup
}

// NewTCPPeer is contructor for TCPPeer
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn: conn,
		outbound: outbound,
		Wg: &sync.WaitGroup{},
	}
}

// Send implements Peer interface
func (p *TCPPeer) Send(b []byte) error {
	_, err := p.Conn.Write(b)
	return err
}

type TCPTransportOpts struct {
	ListenAddr string
	HandshakeFunc HandshakeFunc
	Decoder Decoder
	OnPeer  func(Peer) error
}
//  TCPTransport is 
type TCPTransport struct {
	TCPTransportOpts
	listener      net.Listener
	rpcch chan RPC
}

// NewTCPTransport is constructor for TCPTransport
func NewTCPTransport(opts TCPTransportOpts)  *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch: make(chan RPC),
	}
}

// Dial implements Transport interface
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return err
	}

	go t.handleConn(conn, true)

	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}
	// might be a good place to use defer ??

	go t.startAcceptLoop()

	log.Printf("TCP transport listening on port: %s\n", t.ListenAddr)

	return nil
}

// Close implements the Transport interface
func(t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Consume implements the Transport Interface, which will return
// read-only channel for reading incoming messages received from another
// peer in the network.
func (t* TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}

		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func ()  {
		fmt.Printf("Dropping peer connection: %s\n", err)
		conn.Close()
	}()
	peer := NewTCPPeer(conn, outbound)

	// Handshake with peer
	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	// Read Loop
	rpc := RPC{}
	for {
		err := t.Decoder.Decode(conn, &rpc)
		
		if err != nil {
			fmt.Printf("TCP error: %s\n", err)
			return
		}
		rpc.From = conn.RemoteAddr().String()
		peer.Wg.Add(1)
		fmt.Println("waiting till stream is done")
		t.rpcch <- rpc
		peer.Wg.Wait()
		fmt.Println("stream done continuing normal read loop")
	}
}
