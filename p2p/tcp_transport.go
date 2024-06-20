package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
)

// TCPPeer represents the remote node over a TCP established connection.
type TCPPeer struct {
	// conn is underlying connection of the peer
	conn net.Conn
	// if we dial and retrive a conn => outbound == true
	// if we accept and retrive a conn => outbound == false
	outbound bool
}

// NewTCPPeer is contructor for TCPPeer
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn: conn,
		outbound: outbound,
	}
}

// Close implements Peer interface
func (p *TCPPeer) Close() error {
	return p.conn.Close()
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

	// Responsibility of server to manage peers and not transport
	// mu    sync.RWMutex 
	// peers map[net.Addr]Peer
}

// NewTCPTransport is constructor for TCPTransport
func NewTCPTransport(opts TCPTransportOpts)  *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch: make(chan RPC),
	}
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

		fmt.Printf("new incoming connection %+v\n", conn)
		go t.handleConn(conn)
	}
}


type Temp struct {}

func (t *TCPTransport) handleConn(conn net.Conn) {
	var err error

	defer func ()  {
		fmt.Printf("Dropping peer connection: %s\n", err)
		conn.Close()
	}()
	peer := NewTCPPeer(conn, true)

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
		rpc.From = conn.RemoteAddr()
		t.rpcch <- rpc
	}

	
}
