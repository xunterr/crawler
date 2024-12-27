package dht

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"net"
	"sync"

	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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

func ToNodeWithID(addr string, id []byte) (*Node, error) {
	a, err := net.ResolveTCPAddr("tcp", addr)
	return &Node{a, id}, err
}

var (
	FIND_SUCCESSOR     string = "dht.findSuccessor"
	UPDATE_PREDECESSOR string = "dht.updatePredecessor"
	UPDATE_FINGER      string = "dht.updateFinger"
	GET_SUCC_LIST      string = "dht.getSuccList"
	PING               string = "dht.ping"
)

type DhtConfig struct {
	Addr               string
	SuccListLength     int
	StabilizeInterval  int
	FixFingersInterval int
	VnodeNum           int
}

type DHT struct {
	logger *zap.SugaredLogger

	peer *p2p.Peer

	vnodes map[string]*vnode
	vnMu   sync.Mutex
}

func NewDHT(logger *zap.Logger, peer *p2p.Peer, conf DhtConfig) (*DHT, error) {
	addr, err := net.ResolveTCPAddr("tcp", conf.Addr)
	if err != nil {
		return nil, err
	}

	d := &DHT{
		logger: logger.Sugar(),
		peer:   peer,
		vnodes: make(map[string]*vnode),
	}

	d.registerHandlers()
	d.initVnodes(conf, addr)

	return d, nil
}

func (d *DHT) registerHandlers() {
	d.peer.AddRequestHandler(FIND_SUCCESSOR, d.findSuccessorHandler)
	d.peer.AddRequestHandler(UPDATE_PREDECESSOR, d.updatePredecessorHandler)
	d.peer.AddRequestHandler(GET_SUCC_LIST, d.getSuccListHandler)
	d.peer.AddRequestHandler(PING, d.pingHandler)
}

func (d *DHT) initVnodes(dhtConf DhtConfig, dhtAddr net.Addr) {
	var prevNode *Node
	for i := 0; i < dhtConf.VnodeNum; i++ {
		addr := fmt.Sprintf("%s:%d", dhtConf.Addr, i)
		nodeId := d.MakeKey([]byte(addr))
		node := &Node{
			Id:   nodeId,
			Addr: dhtAddr,
		}

		vnodeConf := vnodeConf{
			SuccListLength:     dhtConf.SuccListLength,
			StabilizeInterval:  dhtConf.StabilizeInterval,
			FixFingersInterval: dhtConf.FixFingersInterval,
		}

		vnode := newVnode(d.logger.Desugar(), node, func(addr string) *p2p.RequestWriter {
			return p2p.NewRequestWriter(d.peer, addr)
		}, &vnodeConf)

		d.vnMu.Lock()
		d.vnodes[string(nodeId)] = vnode
		d.vnMu.Unlock()

		if prevNode != nil {
			go vnode.join(prevNode)
		}

		prevNode = node
	}
}

func (d *DHT) Join(addr string) error {
	firstVnode := fmt.Sprintf("%s:1", addr)
	node, err := ToNodeWithID(addr, d.MakeKey([]byte(firstVnode)))
	if err != nil {
		return err
	}

	d.vnMu.Lock()
	defer d.vnMu.Unlock()
	for _, n := range d.vnodes {
		go n.join(node)
	}

	return nil
}

func (d *DHT) FindSuccessor(key []byte) (*Node, error) {
	vnode := d.findClosestVnode(key)
	return vnode.FindSuccessor(key)
}

func (d *DHT) findClosestVnode(key []byte) *vnode {
	var prev *vnode

	d.vnMu.Lock()
	defer d.vnMu.Unlock()

	for _, v := range d.vnodes {
		if bytes.Compare(v.GetID(), key) == 0 {
			return v
		}

		if prev == nil ||
			Between(v.GetID(), prev.GetID(), key) {
			prev = v
		}
	}

	return prev
}

func (d *DHT) MakeKey(from []byte) []byte {
	id := sha1.New()
	id.Write(from)
	return id.Sum(nil)
}

func (d *DHT) getVnodeFromMetadata(metadata map[string][]byte) (*vnode, bool) {
	nodeId, ok := metadata["to"]
	if !ok {
		return nil, false
	}

	d.vnMu.Lock()
	node, ok := d.vnodes[string(nodeId)]
	d.vnMu.Unlock()
	return node, ok
}

func (d *DHT) findSuccessorHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	vnode, ok := d.getVnodeFromMetadata(ctx.Metadata())
	if !ok {
		rw.Response(false, []byte{})
		return
	}

	msg := &pb.Key{}
	if err := proto.Unmarshal(data, msg); err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	succ, err := vnode.FindSuccessor(msg.Key)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	res := &pb.Node{
		Id:   succ.Id,
		Addr: succ.Addr.String(),
	}

	resBytes, err := proto.Marshal(res)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	rw.Response(true, resBytes)
}

func (d *DHT) updatePredecessorHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	vnode, ok := d.getVnodeFromMetadata(ctx.Metadata())
	if !ok {
		rw.Response(false, []byte{})
		return
	}

	msg := &pb.Node{}
	if err := proto.Unmarshal(data, msg); err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	newPred, err := ToNodeWithID(msg.Addr, msg.Id)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	oldPred := vnode.pred
	vnode.updatePredecessor(newPred)

	res := &pb.Node{
		Id:   oldPred.Id,
		Addr: oldPred.Addr.String(),
	}

	resBytes, err := proto.Marshal(res)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	rw.Response(true, resBytes)
}

func (d *DHT) getSuccListHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	vnode, ok := d.getVnodeFromMetadata(ctx.Metadata())
	if !ok {
		rw.Response(false, []byte{})
		return
	}

	nodes := make([]*pb.Node, len(vnode.succList))
	for i, e := range vnode.succList {
		nodes[i] = &pb.Node{
			Id:   e.Id,
			Addr: e.Addr.String(),
		}
	}

	succList := &pb.SuccList{
		Node: nodes,
	}

	res, err := proto.Marshal(succList)
	if err != nil {
		rw.Response(false, []byte(err.Error()))
		return
	}

	rw.Response(true, res)
}

func (d *DHT) pingHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	rw.Response(true, []byte("PONG"))
}
