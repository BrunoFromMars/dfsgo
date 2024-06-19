package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)
func TestTCPTransport(t *testing.T) {
	opts := TCPTransportOpts {
		ListenAddr: ":3000",
		HandshakeFunc: NoHandShakeFunc,
		Decoder: DefaultDecoder{},
	}
	tr := NewTCPTransport(opts)
	assert.Equal(t, tr.ListenAddr, opts.ListenAddr)

	// Server
	assert.Nil(t, tr.ListenAndAccept())
}
