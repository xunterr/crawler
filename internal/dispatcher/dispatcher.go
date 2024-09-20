package dispatcher

import (
	"bytes"
	"context"
	"crypto/sha1"
	"net"
	"net/url"

	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

type Node struct {
	addr net.Addr
	id   []byte
}

func ToNode(addr string) (*Node, error) {
	id := sha1.New().Sum([]byte(addr))

	a, err := net.ResolveTCPAddr("tcp", addr)
	return &Node{a, id}, err
}

type Dispatcher struct {
	client      *p2p.Client
	self        *Node
	fingerTable []*Node
}

func NewDispatcher(client *p2p.Client, router *p2p.Router, addr string) (*Dispatcher, error) {
	self, err := ToNode(addr)
	if err != nil {
		return nil, err
	}

	d := &Dispatcher{
		client: client,
		self:   self,
	}

	router.AddHandler("dht.findSuccessor", d.findSuccessorHandler)
	return d, nil
}

func (d *Dispatcher) Dispatch(url *url.URL) {
	//hash := sha1.New().Sum([]byte(url.Hostname()))
}

func (d *Dispatcher) findSuccessorHandler(ctx context.Context, c *p2p.Conn) {
	bytes, err := p2p.Read(c)
	if err != nil {
		return
	}

	msg := &pb.DhtFindSuccessor{}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return
	}

	succ, err := d.findSuccessor(msg.Key)
	if err != nil {
		return
	}

	res := &pb.DhtFindSuccessorResult{
		Successor: succ.addr.String(),
	}

	data, err := proto.Marshal(res)
	if err != nil {
		return
	}

	if _, err = c.Write(data); err != nil {
		return
	}
}

func (d *Dispatcher) findSuccessorRPC(node *Node, key []byte) (*Node, error) {
	conn, err := d.client.Dial("dht.findSuccessor", node.addr.String())
	if err != nil {
		return nil, err
	}

	req := &pb.DhtFindSuccessor{
		Key: key,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	resBytes, err := conn.Call(reqBytes)

	res := &pb.DhtFindSuccessorResult{}
	if err = proto.Unmarshal(resBytes, res); err != nil {
		return nil, err
	}

	return ToNode(res.Successor)
}

func (d *Dispatcher) findSuccessor(key []byte) (*Node, error) {
	if len(d.fingerTable) == 0 {
		return d.self, nil
	}

	if bytes.Compare(d.self.id, key) == 0 {
		return d.self, nil
	}

	n := d.findPredecessor(key)

	if bytes.Compare(d.self.id, n.id) == 0 {
		return d.fingerTable[0], nil
	} else {
		return d.findSuccessorRPC(n, key)
	}
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
