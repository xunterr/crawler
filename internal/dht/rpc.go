package dht

import (
	"context"
	"log"
	"net"

	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

var (
	FIND_SUCCESSOR     string = "dht.findSuccessor"
	FIND_CLOSEST       string = "dht.findClosestFinger"
	UPDATE_PREDECESSOR string = "dht.updatePredecessor"
	UPDATE_FINGER      string = "dht.updateFinger"
)

func (d *DHT) registerHandlers(router *p2p.Router) {
	router.AddRequestHandler(FIND_SUCCESSOR, d.findSuccessorHandler)
	router.AddRequestHandler(UPDATE_PREDECESSOR, d.updatePredecessorHandler)
	router.AddRequestHandler(UPDATE_FINGER, d.updateFingerHandler)
}

func (d *DHT) findSuccessorRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	err := p2p.RpcCall(d.peer, node.Addr.String(), FIND_SUCCESSOR, req, res)
	if err != nil {
		return nil, err
	}

	a, err := net.ResolveTCPAddr("tcp", res.Addr)
	return &Node{
		Id:   res.Id,
		Addr: a,
	}, err
}

func (d *DHT) findClosesFingerRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	err := p2p.RpcCall(d.peer, node.Addr.String(), FIND_CLOSEST, req, res)
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
	if err := p2p.RpcCall(d.peer, node.Addr.String(), UPDATE_PREDECESSOR, req, res); err != nil {
		return nil, err
	}

	return ToNode(res.Addr)
}

func (d *DHT) updateFingerRPC(n *Node, finger *Node, i int) error {
	conn, err := d.peer.Dial(n.Addr.String())
	if err != nil {
		return err
	}

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

	log.Printf("Updating %d th finger of node %s", i, n.Addr.String())
	_, err = p2p.Call(conn, req)

	return nil
}

func (d *DHT) findSuccessorHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	msg := &pb.Key{}
	if err := proto.Unmarshal(req.Payload, msg); err != nil {
		log.Println(err.Error())
		rw.Response(false, []byte{})
		return
	}

	succ, err := d.FindSuccessor(msg.Key)
	if err != nil {
		log.Println(err.Error())
		rw.Response(false, []byte{})
		return
	}

	res := &pb.Node{
		Id:   succ.Id,
		Addr: succ.Addr.String(),
	}

	data, err := proto.Marshal(res)
	if err != nil {
		log.Println(err.Error())
		rw.Response(false, []byte{})
		return
	}

	rw.Response(true, data)
}

func (d *DHT) updatePredecessorHandler(ctx context.Context, req *p2p.Request, rw *p2p.ResponseWriter) {
	msg := &pb.Node{}
	if err := proto.Unmarshal(req.Payload, msg); err != nil {
		return
	}

	newPred, err := ToNode(msg.Addr)
	if err != nil {
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
		rw.Response(false, data)
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
