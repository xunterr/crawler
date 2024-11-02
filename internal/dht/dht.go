package dht

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"time"

	p2p "github.com/xunterr/crawler/internal/net"
)

type finger struct {
	id   []byte
	succ *Node
}

type fingerTable []*finger

type Node struct {
	Addr net.Addr
	Id   []byte
}

func ToNode(addr string) (*Node, error) {
	h := sha1.New()
	_, err := h.Write([]byte(addr))
	if err != nil {
		return nil, err
	}

	id := h.Sum(nil)
	a, err := net.ResolveTCPAddr("tcp", addr)
	return &Node{a, id}, err
}

type DHT struct {
	peer        *p2p.Peer
	self        *Node
	fingerTable fingerTable
	succ        *Node
	pred        *Node
}

func NewDHT(peer *p2p.Peer, router *p2p.Router, addr string) (*DHT, error) {
	self, err := ToNode(addr)
	if err != nil {
		return nil, err
	}

	fingerTable := initFingerTable(self)
	d := &DHT{
		peer:        peer,
		self:        self,
		fingerTable: fingerTable,
		succ:        fingerTable[0].succ,
		pred:        self,
	}

	d.registerHandlers(router)

	go func() {
		ticker := time.NewTicker(time.Duration(5_000) * time.Millisecond)
		for range ticker.C {
			d.stabilize()
		}
	}()

	go func() {
		next := 0
		ticker := time.NewTicker(time.Duration(3_000) * time.Millisecond)
		for range ticker.C {
			d.fixFinger(next)
			next = (next + 1) % 160
		}
	}()

	return d, nil
}

func initFingerTable(n *Node) []*finger {
	table := make([]*finger, 160)
	for i := 0; i < 160; i++ {
		fingerId := getFingerId(n.Id, i, 160)
		table[i] = &finger{
			id:   fingerId,
			succ: n,
		}
	}
	return table
}

func getFingerId(n []byte, k, m int) []byte {
	x := big.NewInt(2)
	x.Exp(x, big.NewInt(int64(k)-1), nil)

	y := big.NewInt(2)
	y.Exp(y, big.NewInt(int64(m)), nil)

	res := &big.Int{}
	res.SetBytes(n).Add(res, x).Mod(res, y)
	return res.Bytes()
}

func (d *DHT) GetID() []byte {
	return d.self.Id
}

func (d *DHT) stabilize() error {
	succPred, err := d.updatePredecessorRPC(d.fingerTable[0].succ, d.self)
	if err != nil {
		return err
	}

	if Between(succPred.Id, d.self.Id, d.fingerTable[0].succ.Id) {
		log.Printf("Found new successor %s! Updating...", succPred.Addr.String())
		d.fingerTable[0].succ = succPred
	}
	return nil
}

func (d *DHT) fixFinger(finger int) error {
	newFinger, err := d.FindSuccessor(d.fingerTable[finger].id)
	if err != nil {
		return err
	}

	d.fingerTable[finger].succ = newFinger
	return nil
}

func (d *DHT) updatePredecessor(p *Node) {
	if d.pred == nil || Between(p.Id, d.pred.Id, d.self.Id) {
		log.Println("Found new predecessor! Updating...")
		d.pred = p
	}
}

func (d *DHT) updateFinger(i int, s *Node) {
	if bytes.Compare(s.Id, d.self.Id) == 0 ||
		Between(s.Id, d.self.Id, d.fingerTable[i].succ.Id) {
		d.fingerTable[i].succ = s
	}
	d.displayTable()
}

func (d *DHT) MakeKey(from []byte) []byte {
	id := sha1.New()
	id.Write(from)
	return id.Sum(nil)
}

func (d *DHT) displayTable() {
	for i, e := range d.fingerTable {
		fmt.Printf("%d-... : %s\r\n", i, e.succ.Addr.String())
	}
}

func (d *DHT) Join(n *Node) error {
	succ, err := d.findSuccessorRPC(n, d.self.Id)
	if err != nil {
		return err
	}

	d.fingerTable[0].succ = succ

	d.displayTable()
	return nil
}

func (d *DHT) FindSuccessor(key []byte) (*Node, error) {
	if len(d.fingerTable) == 0 {
		log.Println("No fingers")
		return d.self, nil
	}

	if bytes.Compare(d.self.Id, key) == 0 {
		return d.self, nil
	}

	n := d.findPredecessor(key)

	if bytes.Compare(d.self.Id, n.Id) == 0 {
		return d.fingerTable[0].succ, nil
	} else {
		return d.findSuccessorRPC(n, key)
	}
}

func (d *DHT) findPredecessor(key []byte) *Node {
	for i := len(d.fingerTable) - 1; i > 0; i-- {
		if Between(d.fingerTable[i].succ.Id, d.self.Id, key) {
			return d.fingerTable[i].succ
		}
	}
	return d.self
}

func Between(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) > 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) > 0
	case 0:
		return bytes.Compare(a, key) != 0
	}
	return false
}
