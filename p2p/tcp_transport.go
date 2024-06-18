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

//  TCPTransport is 
type TCPTransport struct {
	listenAddress string
	listener      net.Listener
	shakeHands HandshakeFunc
	decoder Decoder

	mu    sync.RWMutex
	peers map[net.Addr]Peer
}

// NewTCPTransport is constructor for TCPTransport
func NewTCPTransport(listenAddr string)  *TCPTransport {
	return &TCPTransport{
		shakeHands:  NoHandShakeFunc,
		listenAddress: listenAddr,
	}
}


func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.listenAddress)
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

	if err := t.shakeHands(peer); err != nil {
		conn.Close()
		fmt.Printf("TCP Handshake error: %s\n", err)
		return
	}

	// Read Loop
	msg := &Temp{}
	for {
		if err := t.decoder.Decode(conn, msg) ; err != nil {
			fmt.Printf("TCP error: %s\n", err)
			continue
		}
	}

	
}
