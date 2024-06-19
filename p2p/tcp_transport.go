package p2p

import (
	"fmt"
	"net"
	"sync"
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

type TCPTransportOpts struct {
	ListenAddr string
	HandshakeFunc HandshakeFunc
	Decoder Decoder
}
//  TCPTransport is 
type TCPTransport struct {
	TCPTransportOpts
	listener      net.Listener

	mu    sync.RWMutex
	peers map[net.Addr]Peer
}

// NewTCPTransport is constructor for TCPTransport
func NewTCPTransport(opts TCPTransportOpts)  *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
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

	return nil

}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}

		fmt.Printf("new incoming connection %+v\n", conn)
		go t.handleConn(conn)
	}
}


type Temp struct {}

func (t *TCPTransport) handleConn(conn net.Conn) {
	peer := NewTCPPeer(conn, true)

	// Handshake with peer
	if err := t.HandshakeFunc(peer); err != nil {
		conn.Close()
		fmt.Printf("TCP Handshake error: %s\n", err)
		return
	}

	// Read Loop
	msg := &Message{}
	for {
		if err := t.Decoder.Decode(conn, msg) ; err != nil {
			fmt.Printf("TCP error: %s\n", err)
			continue
		}
		msg.From = conn.RemoteAddr()
		fmt.Printf("message: %+v\n", msg)
	}

	
}
