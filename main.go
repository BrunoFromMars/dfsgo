package main

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/BrunoFromMars/dfsgo/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpOpts := p2p.TCPTransportOpts {
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NoHandShakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpOpts)

	fileServerOpts := FileServerOpts {
		StorageRoot:       strings.Trim(listenAddr, ":") + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	fs := NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = fs.OnPeer

	return fs
}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":4000", ":3000")

	go func ()  {
		log.Fatal(s1.Start())
	}()
	
	time.Sleep(time.Second * 2)

	go s2.Start()

	time.Sleep(time.Second * 2)

	// for i := 0; i < 5; i++ {
	// 	data := bytes.NewReader([]byte("my big data file"))
	// 	s2.StoreData(fmt.Sprintf("myprivatedata_%d", i), data)
	// 	time.Sleep(5 * time.Millisecond)
	// }

	// data := bytes.NewReader([]byte("my big data file"))
	// s2.StoreData("coolPicture.jpg", data)

	r, err := s2.GetData("coolPicture.jpg")

	if err != nil {
		log.Fatal(err)
	}
	b, err := io.ReadAll(r)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(b))
}
