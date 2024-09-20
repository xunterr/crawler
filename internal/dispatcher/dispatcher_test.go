package dispatcher

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

func setup() *Dispatcher {

	finger := make([]*Node, 10)

	for i := 1; i < len(finger); i++ {
		id := make([]byte, 4)
		binary.LittleEndian.PutUint32(id, uint32(i*2))

		addr, err := net.ResolveTCPAddr("tcp", "192.168.0.0:6969")
		if err != nil {
			panic(err)
		}

		finger[i] = &Node{
			addr: addr,
			id:   id,
		}
	}

	addr, err := net.ResolveTCPAddr("tcp", "192.168.0.0:6969")
	if err != nil {
		panic(err)
	}

	return &Dispatcher{
		self: &Node{
			addr: addr,
			id:   []byte{0},
		},

		fingerTable: finger,
	}
}

func TestFindPredecessor(t *testing.T) {
	dispatcher := setup()

	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, uint32(16))

	p := dispatcher.findPredecessor(id).id
	t.Logf("Predecessor: %d; Key: %d", p, id)
	if bytes.Compare(p, dispatcher.fingerTable[7].id) != 0 {
		t.FailNow()
	}
}
