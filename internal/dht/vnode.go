package dht

import (
	"bytes"
	"math/big"
	"time"

	p2p "github.com/xunterr/crawler/internal/net"
	"go.uber.org/zap"
)

type vnodeConf struct {
	SuccListLength     int
	StabilizeInterval  int
	FixFingersInterval int
}

type rwProvider func(addr string) *p2p.RequestWriter

type vnode struct {
	logger      *zap.SugaredLogger
	self        *Node
	fingerTable fingerTable
	succList    []*Node
	succ        *Node
	pred        *Node

	rw rwProvider
}

func newVnode(logger *zap.Logger, self *Node, rw rwProvider, conf *vnodeConf) *vnode {
	fingerTable := initFingerTable(self)
	succList := initSuccList(conf.SuccListLength, self)
	v := &vnode{
		logger: logger.Sugar(),

		self:        self,
		fingerTable: fingerTable,
		succList:    succList,
		pred:        self,
		rw:          rw,
	}

	go func() {
		ticker := time.NewTicker(time.Duration(conf.StabilizeInterval) * time.Millisecond)
		for range ticker.C {
			v.stabilize()
		}
	}()

	go func() {
		next := 0
		ticker := time.NewTicker(time.Duration(conf.FixFingersInterval) * time.Millisecond)
		for range ticker.C {
			v.fixFinger(next)
			next = (next + 1) % 160
		}
	}()

	return v
}

func (v *vnode) GetID() []byte {
	return v.self.Id
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

func (d *vnode) join(n *Node) error {
	succ, err := d.findSuccessorRPC(n, d.self.Id)
	if err != nil {
		return err
	}
	d.fingerTable[0].succ = succ

	return nil
}

func (d *vnode) stabilize() error {
	d.refreshSuccessors()
	succPred, err := d.updatePredecessorRPC(d.fingerTable[0].succ, d.self)
	if err != nil {
		d.logger.Infow("Successor is down!", zap.String("node", d.fingerTable[0].succ.Addr.String()))
		return err
	}

	//if is between us and succ and alive OR if our successor is down
	if (Between(succPred.Id, d.self.Id, d.fingerTable[0].succ.Id) && d.isNodeAlive(succPred)) || !d.isNodeAlive(d.fingerTable[0].succ) {
		d.fingerTable[0].succ = succPred
		d.logger.Infow("Found new successor", zap.String("node", d.fingerTable[0].succ.Addr.String()))
	}

	return nil
}

func (d *vnode) fixFinger(finger int) error {
	newFinger, err := d.FindSuccessor(d.fingerTable[finger].id)
	if err != nil {
		return err
	}

	d.fingerTable[finger].succ = newFinger
	return nil
}

func (d *vnode) updatePredecessor(p *Node) {
	if d.pred == nil || Between(p.Id, d.pred.Id, d.self.Id) || !d.isNodeAlive(d.pred) {
		d.logger.Infow("Found new predecessor", zap.String("node", p.Addr.String()))
		d.pred = p
	}
}

func (d *vnode) isNodeAlive(node *Node) bool {
	err := d.pingRPC(node)
	return err == nil
}

func (d *vnode) updateFinger(i int, s *Node) {
	if bytes.Compare(s.Id, d.self.Id) == 0 ||
		Between(s.Id, d.self.Id, d.fingerTable[i].succ.Id) {
		d.fingerTable[i].succ = s
	}
}

func (d *vnode) refreshSuccessors() error {
	currSucc := 0
	for currSucc < len(d.succList) {
		succList, err := d.getSuccListRPC(d.fingerTable[0].succ)
		if err != nil {
			d.logger.Infow("Successor is down!", zap.String("node", d.fingerTable[0].succ.Addr.String()))

			if currSucc == len(d.succList)-1 {
				break
			}
			d.fingerTable[0].succ = d.succList[currSucc+1]

			d.logger.Infow("New successor", zap.String("node", d.fingerTable[0].succ.Addr.String()))

			currSucc++
		} else {
			copy(succList[1:], succList)
			succList[0] = d.fingerTable[0].succ
			d.succList = succList
			break
		}
	}
	return nil
}

func (d *vnode) FindSuccessor(key []byte) (*Node, error) {
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
			return d.fingerTable[0].succ, nil
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

func (d *vnode) findPredecessor(key []byte, step int) *Node {
	step += 1 //+1 for the first predecessor

	var prev *Node

	for i := len(d.fingerTable) - 1; i >= 0; i-- {
		if Between(d.fingerTable[i].succ.Id, d.self.Id, key) {
			if prev == nil || bytes.Compare(prev.Id, d.fingerTable[i].succ.Id) != 0 { //if found new unique predecessor
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
