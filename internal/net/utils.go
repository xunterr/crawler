package net

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
)

func RpcCall(peer *Peer, addr string, scope string, req proto.Message, res proto.Message) error {
	conn, err := peer.Dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	request := &Request{
		Scope:   scope,
		Payload: reqBytes,
	}

	response, err := Call(conn, request)

	return proto.Unmarshal(response.Payload, res)
}

func Call(c net.Conn, req *Request) (*Response, error) {
	data := req.Marshal()
	msg := &Message{
		Length:  uint32(len(data)),
		Type:    RequestMsg,
		Version: 1,
		Data:    data,
	}

	_, err := c.Write(msg.Marshal())
	if err != nil {
		return nil, err
	}

	resMsg, err := ParseMessage(c)
	if err != nil {
		return nil, err
	}

	if resMsg.Type != ResponseMsg {
		return nil, errors.New("Unexpected response type")
	}

	return ParseResponse(resMsg.Data)
}

func readData(r io.Reader) ([]byte, error) {
	length, err := readLength(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)

	n, err := r.Read(buf)
	if err != nil {
		return nil, err
	}

	return buf[:n], err
}

func packMessage(data []byte) []byte {
	msg := make([]byte, len(data)+4)
	binary.BigEndian.PutUint32(msg, uint32(len(data)))
	for i, e := range data {
		msg[i+4] = e
	}

	return msg
}

func unpackMessage(data []byte) ([]byte, []byte) {
	if len(data) < 4 {
		return []byte{}, []byte{}
	}

	length := binary.BigEndian.Uint32(data[:4])
	if uint32(len(data)) < 4+length {
		return []byte{}, []byte{}
	}

	return data[4:length], data[length:]
}
