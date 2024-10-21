package dispatcher

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

func (d *Dispatcher) rpcCall(node *Node, scope string, req proto.Message, res proto.Message) error {
	conn, err := d.client.Dial(scope, node.Addr.String())
	if err != nil {
		return err
	}
	defer conn.Close()

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	resBytes, err := call(conn, reqBytes)

	return proto.Unmarshal(resBytes, res)
}

func call(c *p2p.Conn, data []byte) ([]byte, error) {
	_, err := p2p.WriteMessage(c, data)
	if err != nil {
		return nil, err
	}

	return p2p.ReadMessage(c)
}

func (d *Dispatcher) registerHandlers(router *p2p.Router) {
	router.AddHandler(FIND_SUCCESSOR, d.findSuccessorHandler)
	router.AddHandler(UPDATE_PREDECESSOR, d.updatePredecessorHandler)
	router.AddHandler(UPDATE_FINGER, d.updateFingerHandler)
}

func (d *Dispatcher) findSuccessorRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	err := d.rpcCall(node, FIND_SUCCESSOR, req, res)
	if err != nil {
		return nil, err
	}

	a, err := net.ResolveTCPAddr("tcp", res.Addr)
	return &Node{
		Id:   res.Id,
		Addr: a,
	}, err
}

func (d *Dispatcher) findClosesFingerRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	err := d.rpcCall(node, FIND_CLOSEST, req, res)
	if err != nil {
		return nil, err
	}

	a, err := net.ResolveTCPAddr("tcp", res.Addr)
	return &Node{
		Id:   res.Id,
		Addr: a,
	}, err
}

func (d *Dispatcher) updatePredecessorRPC(n *Node, pred *Node) (*Node, error) {
	req := &pb.Node{
		Id:   pred.Id,
		Addr: pred.Addr.String(),
	}

	res := &pb.Node{}
	if err := d.rpcCall(n, UPDATE_PREDECESSOR, req, res); err != nil {
		return nil, err
	}

	return ToNode(res.Addr)
}

func (d *Dispatcher) updateFingerRPC(n *Node, finger *Node, i int) error {
	conn, err := d.client.Dial(UPDATE_FINGER, n.Addr.String())
	if err != nil {
		return err
	}

	req := &pb.Finger{
		I: int64(i),
		Node: &pb.Node{
			Id:   finger.Id,
			Addr: finger.Addr.String(),
		},
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	log.Printf("Updating %d th finger of node %s", i, n.Addr.String())
	if _, err = p2p.WriteMessage(conn, reqBytes); err != nil {
		return err
	}
	return nil
}

func (d *Dispatcher) findSuccessorHandler(ctx context.Context, c *p2p.Conn) {
	bytes, err := p2p.ReadMessage(c)
	if err != nil {
		log.Println(err.Error())
		return
	}

	msg := &pb.Key{}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		log.Println(err.Error())
		return
	}

	succ, err := d.FindSuccessor(msg.Key)
	if err != nil {
		log.Println(err.Error())
		return
	}

	res := &pb.Node{
		Id:   succ.Id,
		Addr: succ.Addr.String(),
	}

	data, err := proto.Marshal(res)
	if err != nil {
		log.Println(err.Error())
		return
	}

	if _, err = p2p.WriteMessage(c, data); err != nil {
		log.Println(err.Error())
		return
	}
}

func (d *Dispatcher) updatePredecessorHandler(ctx context.Context, c *p2p.Conn) {
	println("new node :3")
	bytes, err := p2p.ReadMessage(c)
	if err != nil {
		return
	}

	msg := &pb.Node{}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return
	}

	newPred, err := ToNode(msg.Addr)
	if err != nil {
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
		return
	}

	if _, err = p2p.WriteMessage(c, data); err != nil {
		return
	}
}

func (d *Dispatcher) updateFingerHandler(ctx context.Context, c *p2p.Conn) {
	bytes, err := p2p.ReadMessage(c)
	if err != nil {
		return
	}

	msg := &pb.Finger{}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return
	}

	node, err := ToNode(msg.Node.Addr)
	if err != nil {
		return
	}

	d.updateFinger(int(msg.I), node)
}
