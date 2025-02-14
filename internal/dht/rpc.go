package dht

import (
	"errors"
	"net"

	p2p "github.com/xunterr/aracno/internal/net"
	pb "github.com/xunterr/aracno/proto"
	"google.golang.org/protobuf/proto"
)

func buildMetadata(node *Node) map[string][]byte {
	return map[string][]byte{
		"to": node.Id,
	}
}

func (v *vnode) findSuccessorRPC(node *Node, key []byte) (*Node, error) {
	req := &pb.Key{
		Key: key,
	}

	res := &pb.Node{}

	meta := buildMetadata(node)
	err := v.rw(node.Addr.String()).WithMetadata(meta).RequestProto(FIND_SUCCESSOR, req, res)
	if err != nil {
		return nil, err
	}

	a, err := net.ResolveTCPAddr("tcp", res.Addr)
	return &Node{
		Id:   res.Id,
		Addr: a,
	}, err
}

func (v *vnode) updatePredecessorRPC(node *Node, pred *Node) (*Node, error) {
	req := &pb.Node{
		Id:   pred.Id,
		Addr: pred.Addr.String(),
	}

	res := &pb.Node{}
	err := v.rw(node.Addr.String()).
		WithMetadata(buildMetadata(node)).
		RequestProto(UPDATE_PREDECESSOR, req, res)
	if err != nil {
		return nil, err
	}

	return ToNodeWithID(res.Addr, res.Id)
}

func (v *vnode) getSuccListRPC(n *Node) ([]*Node, error) {
	req := &p2p.Request{
		Scope:   GET_SUCC_LIST,
		Payload: []byte{},
	}

	res, err := v.rw(n.Addr.String()).
		WithMetadata(buildMetadata(n)).
		Request(req)
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
		list[i], err = ToNodeWithID(e.Addr, e.Id)
		if err != nil {
			return nil, err
		}
	}

	return list, err
}

func (v *vnode) pingRPC(n *Node) error {
	req := &p2p.Request{
		Scope:   PING,
		Payload: []byte("PING"),
	}

	res, err := v.rw(n.Addr.String()).
		WithMetadata(buildMetadata(n)).
		Request(req)
	if err != nil {
		return err
	}

	if res.IsError {
		return errors.New("Node returned error on ping")
	}
	return nil
}
