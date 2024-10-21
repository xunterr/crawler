package net

import (
	"encoding/binary"
	"io"
	"log"
)

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
