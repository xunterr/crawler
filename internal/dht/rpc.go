package dht

import (
	"context"
	"errors"
	"net"

	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

var (
	FIND_SUCCESSOR     string = "dht.findSuccessor"
	UPDATE_PREDECESSOR string = "dht.updatePredecessor"
	UPDATE_FINGER      string = "dht.updateFinger"
	GET_SUCC_LIST      string = "dht.getSuccList"
	PING               string = "dht.ping"
)

func (d *DHT) registerHandlers(router *p2p.Router) {
	router.AddRequestHandler(FIND_SUCCESSOR, d.findSuccessorHandler)
	router.AddRequestHandler(UPDATE_PREDECESSOR, d.updatePredecessorHandler)
	router.AddRequestHandler(UPDATE_FINGER, d.updateFingerHandler)
	router.AddRequestHandler(GET_SUCC_LIST, d.getSuccListHandler)
	router.AddRequestHandler(PING, d.pingHandler)
}

func (d *DHT) findSuccessorRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	err := d.peer.CallProto(node.Addr.String(), FIND_SUCCESSOR, req, res)
	if err != nil {
		return nil, err
	}

	a, err := net.ResolveTCPAddr("tcp", res.Addr)
	return &Node{
		Id:   res.Id,
		Addr: a,
	}, err
}

func (d *DHT) updatePredecessorRPC(node *Node, pred *Node) (*Node, error) {
	req := &pb.Node{
		Id:   pred.Id,
		Addr: pred.Addr.String(),
	}

	res := &pb.Node{}
	if err := d.peer.CallProto(node.Addr.String(), UPDATE_PREDECESSOR, req, res); err != nil {
		return nil, err
	}

	return ToNode(res.Addr)
}

func (d *DHT) updateFingerRPC(n *Node, finger *Node, i int) error {
	f := &pb.Finger{
		I: int64(i),
		Node: &pb.Node{
			Id:   finger.Id,
			Addr: finger.Addr.String(),
		},
	}

	reqBytes, err := proto.Marshal(f)
	if err != nil {
		return err
	}

	req := &p2p.Request{
		Scope:   UPDATE_FINGER,
		Payload: reqBytes,
	}

	_, err = d.peer.Call(n.Addr.String(), req)

	return nil
}

func (d *DHT) getSuccListRPC(n *Node) ([]*Node, error) {
	req := &p2p.Request{
		Scope:   GET_SUCC_LIST,
		Payload: []byte{},
	}

	res, err := d.peer.Call(n.Addr.String(), req)
	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, errors.New(string(res.Payload))
	}

	succList := &pb.SuccList{}
	err = proto.Unmarshal(res.Payload, succList)
	if err != nil {
		return nil, err
	}

	list := make([]*Node, len(succList.Node))
	for i, e := range succList.Node {
		list[i], err = ToNode(e.Addr)
		if err != nil {
			return nil, err
		}
	}

	return list, err
}

func (d *DHT) pingRPC(n *Node) error {
	req := &p2p.Request{
		Scope:   PING,
		Payload: []byte("PING"),
	}

	res, err := d.peer.Call(n.Addr.String(), req)
	if err != nil {
		return err
	}

	if res.IsError {
		return errors.New("Node returned error on ping")
	}
	return nil
}

func (d *DHT) findSuccessorHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	msg := &pb.Key{}
	if err := proto.Unmarshal(req.Payload, msg); err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	succ, err := d.FindSuccessor(msg.Key)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	res := &pb.Node{
		Id:   succ.Id,
		Addr: succ.Addr.String(),
	}

	data, err := proto.Marshal(res)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	rw.Response(true, data)
}

func (d *DHT) updatePredecessorHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	msg := &pb.Node{}
	if err := proto.Unmarshal(req.Payload, msg); err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	newPred, err := ToNode(msg.Addr)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	oldPred := d.pred
	d.updatePredecessor(newPred)

	res := &pb.Node{
		Id:   oldPred.Id,
		Addr: oldPred.Addr.String(),
	}

	data, err := proto.Marshal(res)
	if err != nil {
		d.logger.Errorln(err.Error())
		rw.Response(false, []byte{})
		return
	}

	rw.Response(true, data)
}

func (d *DHT) updateFingerHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	msg := &pb.Finger{}
	if err := proto.Unmarshal(req.Payload, msg); err != nil {
		rw.Response(false, []byte{})
		return
	}

	node, err := ToNode(msg.Node.Addr)
	if err != nil {
		rw.Response(false, []byte{})
		return
	}

	d.updateFinger(int(msg.I), node)
	rw.Response(true, []byte{})
}

func (d *DHT) getSuccListHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	nodes := make([]*pb.Node, len(d.succList))
	for i, e := range d.succList {
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

func (d *DHT) pingHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	rw.Response(true, []byte("PONG"))
}
