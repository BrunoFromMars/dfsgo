package main

import (
	"fmt"
	"log"
	"time"

	"github.com/BrunoFromMars/dfsgo/p2p"
)

func OnPeer(p2p.Peer) error {
	fmt.Println("doing some logic with the peer outside of TCP Transport")
	return nil
}

func main() {
	tcpOpts := p2p.TCPTransportOpts {
		ListenAddr:    ":3000",
		HandshakeFunc: p2p.NoHandShakeFunc,
		Decoder:       p2p.DefaultDecoder{},
		// OnPeer: OnPeer, TODO: OnPeer function
	}
	tcpTransport := p2p.NewTCPTransport(tcpOpts)

	fileServerOpts := FileServerOpts {
		StorageRoot:       "3000_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
	}
	fs := NewFileServer(fileServerOpts)

	go func() {
		time.Sleep(time.Second * 3)
		fs.Stop()
	}()

	if err := fs.Start(); err != nil {
		log.Fatal(err)
	}
}
