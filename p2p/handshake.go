package p2p

// HandeShakeFunc is ... ??
type HandshakeFunc func(Peer) error

func NoHandShakeFunc(Peer) error { return nil }
