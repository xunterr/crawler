package dht

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

func setup() *DHT {

	finger := make([]*Node, 10)

	for i := 1; i < len(finger); i++ {
		id := make([]byte, 4)
		binary.LittleEndian.PutUint32(id, uint32(i*2))

		addr, err := net.ResolveTCPAddr("tcp", "192.168.0.0:6969")
		if err != nil {
			panic(err)
		}

		finger[i] = &Node{
			Addr: addr,
			Id:   id,
		}
	}

	addr, err := net.ResolveTCPAddr("tcp", "192.168.0.0:6969")
	if err != nil {
		panic(err)
	}

	self := &Node{
		Addr: addr,
		Id:   []byte{0},
	}

	return &DHT{
		self:        self,
		fingerTable: initFingerTable(self),
	}
}

func TestFindPredecessor(t *testing.T) {
	dht := setup()

	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, uint32(16))

	p := dht.findPredecessor(id).Id
	t.Logf("Predecessor: %d; Key: %d", p, id)
	if bytes.Compare(p, dht.fingerTable[7].id) != 0 {
		t.FailNow()
	}
}
