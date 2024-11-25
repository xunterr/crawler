package dht

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"time"

	p2p "github.com/xunterr/crawler/internal/net"
	"go.uber.org/zap"
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

type DhtConfig struct {
	Addr               string
	SuccListLength     int
	StabilizeInterval  int
	FixFingersInterval int
}

type DHT struct {
	logger *zap.SugaredLogger

	peer        *p2p.Peer
	self        *Node
	fingerTable fingerTable
	succList    []*Node
	succ        *Node
	pred        *Node
}

func NewDHT(logger *zap.Logger, peer *p2p.Peer, router *p2p.Router, conf DhtConfig) (*DHT, error) {
	self, err := ToNode(conf.Addr)
	if err != nil {
		return nil, err
	}

	fingerTable := initFingerTable(self)
	succList := initSuccList(conf.SuccListLength, self)
	d := &DHT{
		logger: logger.Sugar(),

		peer:        peer,
		self:        self,
		fingerTable: fingerTable,
		succList:    succList,
		succ:        self,
		pred:        self,
	}

	d.registerHandlers(router)

	go func() {
		ticker := time.NewTicker(time.Duration(conf.StabilizeInterval) * time.Millisecond)
		for range ticker.C {
			d.stabilize()
		}
	}()

	go func() {
		next := 0
		ticker := time.NewTicker(time.Duration(conf.FixFingersInterval) * time.Millisecond)
		for range ticker.C {
			d.fixFinger(next)
			next = (next + 1) % 160
		}
	}()

	return d, nil
}

func initSuccList(length int, succ *Node) []*Node {
	list := make([]*Node, length)
	for i := 0; i < length; i++ {
		list[i] = succ
	}
	return list
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
	d.refreshSuccessors()
	succPred, err := d.updatePredecessorRPC(d.succ, d.self)
	if err != nil {
		d.logger.Infow("Successor is down!", zap.String("node", d.succ.Addr.String()))
		return err
	}

	//if is between us and succ and alive OR if our successor is down
	if (Between(succPred.Id, d.self.Id, d.succ.Id) && d.isNodeAlive(succPred)) || !d.isNodeAlive(d.succ) {
		d.logger.Infow("Found new successor", zap.String("node", d.succ.Addr.String()))
		d.succ = succPred
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
	if d.pred == nil || Between(p.Id, d.pred.Id, d.self.Id) || !d.isNodeAlive(d.pred) {
		d.logger.Infow("Found new predecessor", zap.String("node", p.Addr.String()))
		d.pred = p
	}
}

func (d *DHT) isNodeAlive(node *Node) bool {
	err := d.pingRPC(node)
	return err == nil
}

func (d *DHT) updateFinger(i int, s *Node) {
	if bytes.Compare(s.Id, d.self.Id) == 0 ||
		Between(s.Id, d.self.Id, d.fingerTable[i].succ.Id) {
		d.fingerTable[i].succ = s
	}
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

func (d *DHT) displaySuccList() {
	fmt.Println("-----------------------")
	for i, e := range d.succList {
		fmt.Printf("%d | %s\n", i, e.Addr.String())
	}
	fmt.Println("-----------------------")
}

func (d *DHT) displayPointers() {
	fmt.Println("-----------------------")
	fmt.Printf("pred | %s\n", d.pred.Addr.String())
	fmt.Printf("succ | %s\n", d.succ.Addr.String())
	fmt.Println("-----------------------")
}

func (d *DHT) Join(n *Node) error {
	succ, err := d.findSuccessorRPC(n, d.self.Id)
	if err != nil {
		return err
	}
	d.succ = succ

	return nil
}

func (d *DHT) refreshSuccessors() error {
	currSucc := 0
	for currSucc < len(d.succList) {
		succList, err := d.getSuccListRPC(d.succ)
		if err != nil {
			d.logger.Infow("Successor is down!", zap.String("node", d.succ.Addr.String()))

			if currSucc == len(d.succList)-1 {
				break
			}
			d.succ = d.succList[currSucc+1]

			d.logger.Infow("New successor", zap.String("node", d.succ.Addr.String()))

			currSucc++
		} else {
			copy(succList[1:], succList)
			succList[0] = d.succ
			d.succList = succList
			break
		}
	}
	return nil
}

func (d *DHT) FindSuccessor(key []byte) (*Node, error) {
	if len(d.fingerTable) == 0 {
		d.logger.Warnln("No fingers")
		return d.self, nil
	}

	if bytes.Compare(d.self.Id, key) == 0 {
		return d.self, nil
	}

	step := 0
	for {
		n := d.findPredecessor(key, step)

		if bytes.Compare(d.self.Id, n.Id) == 0 {
			return d.succ, nil
		} else {
			succ, err := d.findSuccessorRPC(n, key)
			if err != nil {
				step++
				continue
			}
			return succ, nil
		}
	}
}

func (d *DHT) findPredecessor(key []byte, step int) *Node {
	step += 1 //+1 for the first predecessor

	prev := d.fingerTable[len(d.fingerTable)-1].succ

	for i := len(d.fingerTable) - 1; i > 0; i-- {
		if Between(d.fingerTable[i].succ.Id, d.self.Id, key) {
			if bytes.Compare(prev.Id, d.fingerTable[i].succ.Id) != 0 { //if found new unique predecessor
				step--
				prev = d.fingerTable[i].succ
			}

			if step == 0 {
				return d.fingerTable[i].succ
			}
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
