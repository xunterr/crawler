package dispatcher

import (
	"bytes"
	"net"
	"net/url"
)

type Node struct {
	addr net.IP
	id   []byte
}

type Dispatcher struct {
	self        *Node
	fingerTable []*Node
}

func (d *Dispatcher) Dispatch(url *url.URL) {
	//hash := sha1.New().Sum([]byte(url.Hostname()))
}

func (d *Dispatcher) findSuccessor(key []byte) *Node {
	if len(d.fingerTable) == 0 {
		return d.self
	}

	if bytes.Compare(d.self.id, key) == 0 {
		return d.self
	}

	n := d.findPredecessor(key)

	if bytes.Compare(d.self.id, n.id) == 0 {
		return d.fingerTable[0]
	} else {
		//	s := dial(n)
		//	s.findSuccessor(key)
	}

	return n
}

func (d *Dispatcher) findPredecessor(key []byte) *Node {
	for i := len(d.fingerTable) - 1; i > 0; i-- {
		if bytes.Compare(d.fingerTable[i].id, key) < 0 &&
			bytes.Compare(d.fingerTable[i].id, d.self.id) > 0 {
			return d.fingerTable[i]
		}
	}
	return d.self
}
