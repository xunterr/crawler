package net

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
)

type MessageType uint8

const (
	RequestMsg MessageType = iota
	ResponseMsg
	StreamMsg
)

type Message struct {
	Length   uint32
	Version  uint8
	Type     MessageType
	Metadata map[string][]byte
	Data     []byte
}

type Data interface {
	Size() int
	Marshal() []byte
}

type Request struct {
	Scope   string
	Payload []byte
}

type Response struct {
	IsError bool
	Payload []byte
}

type Stream struct {
	Scope string
}

func ParseMessage(r io.Reader) (*Message, error) {
	length, version, msgType, err := readHeader(r)
	if err != nil {
		return nil, err
	}

	body := make([]byte, length)
	n, err := io.ReadFull(r, body)
	if n < int(length) {
		log.Println("Fewer bytes where read than declared")
	}

	metadata, next := readMetadata(body)
	data := body[next:]

	message := &Message{
		Length:   length,
		Version:  version,
		Type:     msgType,
		Metadata: metadata,
		Data:     data,
	}

	return message, err
}

func readHeader(r io.Reader) (uint32, uint8, MessageType, error) { //length, version, MessageType, error
	buf := make([]byte, 6)
	n, err := r.Read(buf)
	if err != nil {
		return 0, 0, 0, err
	}

	if n < 6 {
		return 0, 0, 0, nil
	}

	length := binary.BigEndian.Uint32(buf[:4])
	version := buf[4]
	msgType := MessageType(buf[5])

	return length, version, msgType, nil
}

func readMetadata(data []byte) (map[string][]byte, uint) {
	length := data[0]

	metadata := make(map[string][]byte)

	var curr uint = 1
	for i := 0; i < int(length); i++ {
		key, next := readString(data[curr:])
		next += curr

		dataLen := binary.BigEndian.Uint32(data[next : next+4])
		valStart := next + 4
		valEnd := uint32(valStart) + dataLen
		value := data[valStart:valEnd]

		metadata[key] = value

		curr = uint(valEnd)
	}
	return metadata, curr
}

func readLength(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf[:n]), err
}

func ParseRequest(data []byte) (*Request, error) {
	scope, n := readString(data)
	payload := data[n:]
	return &Request{
		Scope:   scope,
		Payload: payload,
	}, nil
}

func readString(data []byte) (string, uint) {
	curr := 0
	for curr < len(data) && data[curr] != '\n' {
		curr++
	}
	str := string(data[:curr])
	return str, uint(curr + 1)
}

func ParseResponse(data []byte) (*Response, error) {
	if len(data) < 1 {
		return nil, errors.New(fmt.Sprintf("Invalid data size. Have: %d, want >= 1", len(data)))
	}

	isErr := data[0] != 0
	payload := data[1:]
	return &Response{
		IsError: isErr,
		Payload: payload,
	}, nil
}

func ParseStream(data []byte) (*Stream, error) {
	scope, _ := readString(data)
	return &Stream{scope}, nil
}

func (m *Message) Marshal() []byte {
	res := make([]byte, 6)
	metaBytes := m.marshalMetadata()

	length := uint32(len(m.Data) + len(metaBytes))
	binary.BigEndian.PutUint32(res, length)

	res[4] = m.Version
	res[5] = byte(m.Type)

	res = append(res, metaBytes...)
	res = append(res, m.Data...)

	return res
}

func (m *Message) marshalMetadata() []byte {
	res := make([]byte, 1)
	res[0] = byte(len(m.Metadata))

	for k, v := range m.Metadata {
		res = append(res, []byte(k)...)
		res = append(res, '\n')

		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(v)))
		res = append(res, lenBytes...)
		res = append(res, v...)
	}
	return res
}

func (r *Request) Size() int {
	return len(r.Payload) + len(r.Scope)
}

func (r *Request) Marshal() []byte {
	var res []byte
	res = append(res, []byte(r.Scope)...)
	res = append(res, '\n')
	res = append(res, r.Payload...)
	return res
}

func (r *Response) Size() int {
	return len(r.Payload) + 1 //+1 for bool
}

func (r *Response) Marshal() []byte {
	var res []byte
	var isErr byte

	if r.IsError {
		isErr = 1
	} else {
		isErr = 0
	}

	res = append(res, isErr)
	res = append(res, r.Payload...)
	return res
}

func (s *Stream) Size() int {
	return len(s.Scope)
}

func (s *Stream) Marshal() []byte {
	return []byte(s.Scope)
}
