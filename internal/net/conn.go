package net

import (
	"net"

	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

type Conn struct {
	Remote net.Addr
	Scope  string
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
