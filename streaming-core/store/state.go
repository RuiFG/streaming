package store

import "sync"

type StateSerializer[T any] func(T) []byte
type StateDeserializer[T any] func([]byte) T
type StateInitializer[T any] func() T

type StateDescriptor[T any] struct {
	Key          string
	Initializer  StateInitializer[T]
	Serializer   StateSerializer[T]
	Deserializer StateDeserializer[T]
}

type state[T any] struct {
	refer        *T
	mutex        *sync.RWMutex
	serializer   StateSerializer[T]
	deserializer StateDeserializer[T]
}

func (r *state[T]) mirror() mirrorState {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return mirrorState{
		Type:    NonParallelizeState,
		Content: r.serializer(*r.refer),
	}
}
