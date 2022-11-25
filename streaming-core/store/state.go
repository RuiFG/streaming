package store

import (
	"github.com/pkg/errors"
	"sync"
)

type StateSerializer[T any] func(T) ([]byte, error)
type StateDeserializer[T any] func([]byte) (T, error)
type StateInitializer[T any] func() T

type StateSerializePostProcessor[T any] func([]byte, error) ([]byte, error)
type StateDeserializePostProcessor[T any] func(T, error) (T, error)

type StateDescriptor[T any] struct {
	Key          string
	Initializer  StateInitializer[T]
	Serializer   StateSerializer[T]
	Deserializer StateDeserializer[T]
}

type state[T any] struct {
	pointer      *T
	mutex        *sync.RWMutex
	serializer   StateSerializer[T]
	deserializer StateDeserializer[T]
}

func (s *state[T]) Pointer() *T {
	return s.pointer
}

func (s *state[T]) Locker() *sync.RWMutex {
	return s.mutex
}

func (s *state[T]) Clear() {
	return
}

func (s *state[T]) mirror() (mirrorState, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if bytes, err := s.serializer(*s.pointer); err != nil {
		return mirrorState{}, errors.WithMessage(err, "failed to serialize state")
	} else {
		return mirrorState{
			Type:    NonParallelizeState,
			Payload: bytes,
		}, err
	}
}
