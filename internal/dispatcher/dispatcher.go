package dispatcher

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/url"
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

type Dispatcher struct {
	client      *p2p.Client
	self        *Node
	fingerTable fingerTable
	succ        *Node
	pred        *Node
}

func NewDispatcher(client *p2p.Client, router *p2p.Router, addr string) (*Dispatcher, error) {
	self, err := ToNode(addr)
	if err != nil {
		return nil, err
	}

	fingerTable := initFingerTable(self)
	d := &Dispatcher{
		client:      client,
		self:        self,
		fingerTable: fingerTable,
		succ:        fingerTable[0].succ,
		pred:        self,
	}

	d.registerHandlers(router)

	go func() {
		ticker := time.NewTicker(time.Duration(10_000) * time.Millisecond)
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

func (d *Dispatcher) GetID() []byte {
	return d.self.Id
}

func (d *Dispatcher) stabilize() error {
	succPred, err := d.updatePredecessorRPC(d.fingerTable[0].succ, d.self)
	if err != nil {
		return err
	}

	if Between(succPred.Id, d.self.Id, d.fingerTable[0].succ.Id) {
		log.Println("Found new successor! Updating...")
		d.displayTable()
		d.fingerTable[0].succ = succPred
	}

	return nil
}

func (d *Dispatcher) fixFinger(finger int) error {
	newFinger, err := d.FindSuccessor(d.fingerTable[finger].id)
	if err != nil {
		return err
	}

	log.Printf("Fixing finger %d...", finger)
	d.displayTable()
	d.fingerTable[finger].succ = newFinger
	return nil
}

func (d *Dispatcher) updatePredecessor(p *Node) {
	if d.pred == nil || Between(p.Id, d.pred.Id, d.self.Id) {
		log.Println("Found new predecessor! Updating...")
		d.pred = p
	}
}

func (d *Dispatcher) updateFinger(i int, s *Node) {
	if bytes.Compare(s.Id, d.self.Id) == 0 ||
		Between(s.Id, d.self.Id, d.fingerTable[i].succ.Id) {
		d.fingerTable[i].succ = s
	}
	d.displayTable()
}

func (d *Dispatcher) Dispatch(url *url.URL) {
	//hash := sha1.New().Sum([]byte(url.Hostname()))
}

func (d *Dispatcher) displayTable() {
	for i, e := range d.fingerTable {
		fmt.Printf("%d-... : %s\r\n", i, e.succ.Addr.String())
	}
}

func (d *Dispatcher) Join(n *Node) error {
	succ, err := d.findSuccessorRPC(n, d.self.Id)
	if err != nil {
		return err
	}

	d.fingerTable[0].succ = succ

	d.displayTable()
	return nil
}

func (d *Dispatcher) FindSuccessor(key []byte) (*Node, error) {
	if len(d.fingerTable) == 0 {
		log.Println("No fingers")
		return d.self, nil
	}

	if bytes.Compare(d.self.Id, key) == 0 {
		return d.self, nil
	}

	n := d.findPredecessoor(key)

	log.Printf("Finger table length: %d", len(d.fingerTable))

	log.Printf("Predecessor is %v!", n.Id)
	if bytes.Compare(d.self.Id, n.Id) == 0 {
		log.Println("Predecessor is us!")
		return d.fingerTable[0].succ, nil
	} else {
		log.Println("Lookup!")
		return d.findSuccessorRPC(n, key)
	}
}

func (d *Dispatcher) findPredecessoor(key []byte) *Node {
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
