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
	Metadata map[string]string
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
	length, err := readLength(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length+2)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	if uint32(n) != length+2 {
		log.Printf("Length of buf: %d", len(buf))
		log.Printf("Actual data length (%d) is less than declared (%d)", n, length)
	}

	version := buf[0]
	msgType := MessageType(buf[1])

	var data []byte
	if len(buf) > 2 {
		data = buf[2:]
	}

	message := &Message{
		Length:  length,
		Version: version,
		Type:    msgType,
		Data:    data,
	}

	return message, err
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
	n, scope := readString(data)
	payload := data[n:]
	return &Request{
		Scope:   scope,
		Payload: payload,
	}, nil
}

func readString(data []byte) (uint, string) {
	curr := 0
	for curr < len(data) && data[curr] != '\n' {
		curr++
	}
	str := string(data[:curr])
	return uint(curr + 1), str
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
	_, scope := readString(data)
	return &Stream{scope}, nil
}

func (m *Message) Marshal() []byte {
	res := make([]byte, 6)

	length := uint32(len(m.Data))
	binary.BigEndian.PutUint32(res, length)

	res[4] = m.Version
	res[5] = byte(m.Type)
	res = append(res, m.Data...)

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
