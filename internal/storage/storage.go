package storage

import "errors"

var NoSuchKeyError error = errors.New("No such key")

type Storage[K comparable, V any] interface {
	Get(K) (V, error)
	Put(V) error
	Delete(K) error
}
