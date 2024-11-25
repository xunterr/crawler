package net

import (
	"encoding/binary"
	"io"
)

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
