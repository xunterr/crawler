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

		finger[i] = &Node{
			addr: net.ParseIP("192.168.0.0"),
			id:   id,
		}
	}

	return &Dispatcher{
		self: &Node{
			addr: net.ParseIP("192.168.0.1"),
			id:   []byte{0},
		},

		fingerTable: finger,
	}
}

func TestFindSuccessor(t *testing.T) {
	dispatcher := setup()

	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, uint32(2))

	succ := dispatcher.findSuccessor(id)

	if succ != dispatcher.fingerTable[0] {
		t.Logf("Successor for key %d is %d", id, succ.id)
		t.Fail()
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
