package net

type MessageType uint32

const (
	Request MessageType = iota
	Response
	Stream
	Error
)

type Message struct {
	Length  uint32
	Version uint8
	Type    MessageType
	Data    chan []byte
}
