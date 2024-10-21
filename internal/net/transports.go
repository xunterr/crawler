package net

import (
	"net"
	"sync"

	"github.com/hashicorp/yamux"
	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

type Conn struct {
	Remote net.Addr
	Scope  string
	buff   []byte
	stream net.Conn
}

func (c *Conn) Read(b []byte) (n int, err error) {
	return c.stream.Read(b)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.stream.Write(p)
}

func (c *Conn) Close() error {
	return c.stream.Close()
}

type Client struct {
	sessions map[string]*yamux.Session
	mu       sync.Mutex
}

func NewClient() *Client {
	return &Client{
		sessions: make(map[string]*yamux.Session),
	}
}

func (c *Client) Dial(scope string, addr string) (*Conn, error) {
	c.mu.Lock()
	session, ok := c.sessions[addr]
	c.mu.Unlock()
	if !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		session, err = yamux.Client(conn, nil)
		if err != nil {
			return nil, err
		}
	}

	stream, err := session.Open()
	if err != nil {
		return nil, err
	}

	sendHeader(scope, stream)

	return &Conn{
		Remote: session.Addr(),
		Scope:  scope,
		stream: stream,
	}, nil
}

func sendHeader(scope string, conn net.Conn) error {
	msg := &pb.Header{
		Scope: scope,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = WriteMessage(conn, data)
	return err
}

func readHeader(conn net.Conn) (*pb.Header, error) {
	data, err := ReadMessage(conn)
	if err != nil {
		return nil, err
	}

	msg := &pb.Header{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return msg, nil
}
