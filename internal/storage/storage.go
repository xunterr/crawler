package storage

import "errors"

var NoSuchKeyError error = errors.New("No such key")

type Storage[V any] interface {
	Get(string) (V, error)
	Put(string, V) error
	Delete(string) error
}
