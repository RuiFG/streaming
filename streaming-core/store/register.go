package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

var (
	ErrStateTypeMismatch = fmt.Errorf("state type error")
)

func RegisterOrGet[T any](controller Controller, descriptor StateDescriptor[T]) (StateController[T], error) {
	if load, ok := controller.Load(descriptor.Key); !ok {
		vs := &state[T]{
			pointer:      new(T),
			mutex:        &sync.RWMutex{},
			serializer:   descriptor.Serializer,
			deserializer: descriptor.Deserializer,
		}
		*vs.pointer = descriptor.Initializer()
		controller.Store(descriptor.Key, vs)
		return vs, nil
	} else {
		switch l := load.(type) {
		case mirrorState:
			if l.Type == NonParallelizeState {
				vs := &state[T]{
					pointer:      new(T),
					mutex:        &sync.RWMutex{},
					serializer:   descriptor.Serializer,
					deserializer: descriptor.Deserializer}
				if t, err := descriptor.Deserializer(l.Payload); err != nil {
					return nil, fmt.Errorf("failed to deserialize state: %w", err)
				} else {
					*vs.pointer = t
					controller.Store(descriptor.Key, vs)
				}
				return vs, nil
			} else {
				return nil, ErrStateTypeMismatch
			}
		case *state[T]:
			return l, nil
		default:
			return nil, ErrStateTypeMismatch
		}
	}
}

// GobRegisterOrGet will use gob decode or encode state, so state should expose fields
func GobRegisterOrGet[T any](controller Controller, key string, initializer StateInitializer[T],
	serializePostProcessor StateSerializePostProcessor[T],
	deserializePostProcessor StateDeserializePostProcessor[T]) (StateController[T], error) {
	return RegisterOrGet[T](controller, GenerateGobStateDescriptor[T](key, initializer, serializePostProcessor, deserializePostProcessor))
}

func GenerateGobStateDescriptor[T any](key string, initializer StateInitializer[T],
	serializePostProcessor StateSerializePostProcessor[T],
	deserializePostProcessor StateDeserializePostProcessor[T]) StateDescriptor[T] {
	if serializePostProcessor == nil {
		serializePostProcessor = func(i []byte, err error) ([]byte, error) {
			return i, err
		}
	}
	if deserializePostProcessor == nil {
		deserializePostProcessor = func(v T, err error) (T, error) { return v, err }
	}
	return StateDescriptor[T]{
		Key:         key,
		Initializer: initializer,
		Serializer: func(v T) ([]byte, error) {
			var buffer bytes.Buffer
			decoder := gob.NewEncoder(&buffer)
			if err := decoder.Encode(&v); err != nil {
				return serializePostProcessor(nil, fmt.Errorf("failed to encode state: %w", err))
			}
			return serializePostProcessor(buffer.Bytes(), nil)
		},
		Deserializer: func(v []byte) (T, error) {
			vPointer := new(T)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(vPointer); err != nil {
				return deserializePostProcessor(*vPointer, fmt.Errorf("failed to decode gob bytes: %w", err))
			} else {
				return deserializePostProcessor(*vPointer, nil)
			}
		},
	}
}
