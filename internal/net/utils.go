package net

import (
	"bytes"
	"io"
)

func Read(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}
