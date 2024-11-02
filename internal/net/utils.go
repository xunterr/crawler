package net

import (
	"encoding/binary"
	"io"
	"log"
	"net"

	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

func RpcCall(peer *Peer, addr string, scope string, req proto.Message, res proto.Message) error {
	conn, err := peer.Dial(scope, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	resBytes, err := Call(conn, reqBytes)

	return proto.Unmarshal(resBytes, res)
}

func Call(c *Conn, data []byte) ([]byte, error) {
	_, err := WriteMessage(c, data)
	if err != nil {
		return nil, err
	}

	return ReadMessage(c)
}

func WriteMessage(w io.Writer, data []byte) (int, error) {
	packed := packMessage(data)
	return w.Write(packed)
}

func ReadMessage(r io.Reader) ([]byte, error) {
	length, err := readLength(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	n, err := r.Read(buf)
	if err != nil {
		return nil, err
	}

	if uint32(n) != length {
		log.Printf("Actual data length (%d) is less than declared (%d)", n, length)
	}

	return buf[:n], err
}

func readLength(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(buf[:n]), err
}

func packMessage(data []byte) []byte {
	msg := make([]byte, len(data)+4)
	binary.LittleEndian.PutUint32(msg, uint32(len(data)))
	for i, e := range data {
		msg[i+4] = e
	}

	return msg
}

func unpackMessage(data []byte) ([]byte, []byte) {
	if len(data) < 4 {
		return []byte{}, []byte{}
	}

	length := binary.LittleEndian.Uint32(data[:4])
	if uint32(len(data)) < 4+length {
		return []byte{}, []byte{}
	}

	return data[4:length], data[length:]
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
