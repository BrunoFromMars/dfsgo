package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/BrunoFromMars/dfsgo/p2p"
)

type FileServerOpts struct {
	ListenAddr        string
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers map[string]p2p.Peer

	store *Store
	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}
	return &FileServer{
		FileServerOpts: opts,

		peers:          make(map[string]p2p.Peer),

		store:          NewStore(storeOpts),
		quitch:         make(chan struct{}),
	}
}

func (fs *FileServer) handleMessage(from string, msg *Message) error {
	switch v :=  msg.Payload.(type) {
	case MessageStoreFile:
		return fs.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return fs.handleMessageGetFile(from, v)
	}
	

	return nil
}

func (fs *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {

	if !fs.store.Has(msg.Key) {
		return fmt.Errorf("need to serve file (%s) does not exist on disk", msg.Key)
	}

	fmt.Printf("serving file (%s) at %s server, serving over network\n", msg.Key, fs.ListenAddr)
	r, err := fs.store.Read(msg.Key)

	if err != nil {
		return err
	}
	// Open up a stream with connection

	peer, ok := fs.peers[from]

	if !ok {
		return fmt.Errorf("peer %s not in the map", from)
	}

	n, err := io.Copy(peer, r)

	if err != nil {
		return err
	}

	fmt.Printf("written %d bytes over the network\n", n)

	return nil;
}

func (fs *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	
	peer, ok := fs.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := fs.store.Write(msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}
	fmt.Printf("written %d bytes to disk\n", n)
	peer.(*p2p.TCPPeer).Wg.Done()

	return nil
}

func (fs *FileServer) bootStrapNetwork() error {
	for _, addr := range fs.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}

		go func (addr string)  {
			fmt.Println("attempting to connect with remote: ", addr)
			if err := fs.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}
	return nil
}

func (fs *FileServer) Start() error {
	if err := fs.Transport.ListenAndAccept(); err != nil {
		return err
	}

	if len(fs.BootstrapNodes) != 0 {
		fs.bootStrapNetwork()
	}

	fs.loop()

	return nil
}

func (fs *FileServer) Stop() {
	close(fs.quitch)
}

func (fs *FileServer) StoreData(key string, r io.Reader) error {

	// 1. Store this file to disk
	// 2. Broadcast this file to all known peers in the network

	var (
		fileBuffer = new(bytes.Buffer)
		tee = io.TeeReader(r, fileBuffer)
	)
	size, err := fs.store.Write(key, tee)

	if err != nil {
		return err
	}
	
	msg :=  Message {
		Payload:  MessageStoreFile{
			Key : key,
			Size: size,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Second * 3)

	// TODO: use a multiwriter here

	for _, peer := range fs.peers {
		n, err := io.Copy(peer, fileBuffer )
		if err!= nil {
			return err
		}

		fmt.Println("received and written bytes to disk", n)
	}

	return nil
	
}

type MessageGetFile struct {
	Key string
}


func (fs *FileServer) GetData( key string) (io.Reader, error) {
	if fs.store.Has(key) {
		return fs.store.Read(key)
	}
	fmt.Printf("dont have file (%s) locally, fetching from network\n", key)

	msg := Message {
		Payload: MessageGetFile {
			Key: key,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return nil, err;
	}

	// Open up a stream to read data from every peer

	for _, peer := range fs.peers {
		fmt.Println("receiving stream from peer: ", peer.RemoteAddr())
		fileBuffer := new(bytes.Buffer)
		n, err := io.CopyN(fileBuffer, peer, 20)
		if err != nil {
			return nil, err
		}

		fmt.Println("received bytes over the network: ", n)
		fmt.Println(fileBuffer.String())

	}


	select {}


	return nil, nil
}

func (fs *FileServer) OnPeer(peer p2p.Peer) error {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	fs.peers[peer.RemoteAddr().String()] = peer

	log.Printf("connected with remote %s", peer.RemoteAddr())

	return nil
	
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key string
	Size int64
}


func (fs *FileServer) stream(msg *Message) error  {
	// buf := new(bytes.Buffer)

	peers := []io.Writer{}

	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)

	
	return gob.NewEncoder(mw).Encode(msg)
}

func (fs *FileServer) broadcast(msg *Message) error {

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range fs.peers {
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FileServer ) loop() {

	defer func ()  {
		log.Println("file server stopped due to error")
		fs.Transport.Close()
	}()

	for {
		select {
		case rpc := <-fs.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader( rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ",err)
			}

			if err := fs.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ",err)
			}

		case <-fs.quitch:
			return
		}
	}
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})

}