package storage

import (
	"errors"
)

var NoNextItem error = errors.New("No next item available")

type Queue[T any] interface {
	Push(T) error
	Peek() (T, error)
	Pop() (T, error)
	Len() int
}
