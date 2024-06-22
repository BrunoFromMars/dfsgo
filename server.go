package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/BrunoFromMars/dfsgo/p2p"
)

type FileServerOpts struct {
	ID                string
	EncKey 			  []byte
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

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		peers:          make(map[string]p2p.Peer),
		quitch:         make(chan struct{}),
	}
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
	size, err := fs.store.Write(fs.ID, key, tee)

	if err != nil {
		return err
	}
	
	msg :=  Message {
		Payload:  MessageStoreFile{
			ID:   fs.ID,
			Key:  hashKey(key),
			Size: size + 16,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)

	peers := []io.Writer{}

	for _, peer :=range fs.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})
	n, err := copyEncrypt(fs.EncKey, fileBuffer, mw)

	if err != nil {
		return err
	}

	fmt.Printf("[%s] received and written (%d) bytes to disk\n", fs.Transport.Addr(), n)

	return nil
}

func (fs *FileServer) GetData( key string) (io.Reader, error) {
	if fs.store.Has(fs.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n",fs.Transport.Addr(), key)
		_, r, err := fs.store.Read(fs.ID, key)
		return r, err
	}
	
	fmt.Printf("[%s] dont have file (%s) locally, fetching from network...\n", fs.Transport.Addr(), key)

	msg := Message {
		Payload: MessageGetFile {
			ID: fs.ID,
			Key: key,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return nil, err;
	}

	// Open up a stream to read data from every peer
	time.Sleep(time.Millisecond * 500)

	for _, peer := range fs.peers {
		// First read the file size so we can limit the amount of bytes that we read
		// from the connection so it will not keep hanging
		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)

		n, err := fs.store.WriteDecrypt(fs.EncKey, fs.ID, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}
		fmt.Printf("[%s] received (%d) bytes over the network from (%s)\n",fs.Transport.Addr(), n, peer.RemoteAddr())
		peer.CloseStream()

	}

	_, r, err := fs.store.Read(fs.ID, key)
	return r, err
}

func (fs *FileServer) OnPeer(peer p2p.Peer) error {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	fs.peers[peer.RemoteAddr().String()] = peer

	log.Printf("connected with remote %s", peer.RemoteAddr())

	return nil
	
}

func (fs *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range fs.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
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

	if !fs.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) but it does not exist on disk", fs.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serving file (%s) serving over network\n", fs.Transport.Addr(), msg.Key )

	fileSize, r, err := fs.store.Read(msg.ID, msg.Key)

	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readClose")
		defer rc.Close()
	}
	// Open up a stream with connection

	peer, ok := fs.peers[from]

	if !ok {
		return fmt.Errorf("peer %s not in the map", from)
	}

	// First send the "incomingstream" byte to the peer and then we can send
	// the file size as an int64
	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian,fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) bytes over the network to %s \n", fs.Transport.Addr(), n, 
	from)

	return nil;
}

func (fs *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	
	peer, ok := fs.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := fs.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}
	fmt.Printf("[%s] written %d bytes to disk\n", fs.Transport.Addr(), n)
	peer.CloseStream()

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