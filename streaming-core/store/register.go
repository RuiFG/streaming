package store

import (
	"fmt"
	"sync"
)

var (
	ErrStateTypeMismatch = fmt.Errorf("state type error")
)

func RegisterOrGet[T any](controller Controller, descriptor StateDescriptor[T]) (*T, *sync.RWMutex, error) {
	var nilRefer *T
	if load, ok := controller.Load(descriptor.Key); !ok {
		vs := &state[T]{
			refer:        new(T),
			mutex:        &sync.RWMutex{},
			serializer:   descriptor.Serializer,
			deserializer: descriptor.Deserializer,
		}
		*vs.refer = descriptor.Initializer()
		controller.Store(descriptor.Key, vs)
		return vs.refer, vs.mutex, nil
	} else {
		switch l := load.(type) {
		case mirrorState:
			if l.Type == NonParallelizeState {
				vs := &state[T]{
					refer:        new(T),
					mutex:        &sync.RWMutex{},
					serializer:   descriptor.Serializer,
					deserializer: descriptor.Deserializer}

				*vs.refer = descriptor.Deserializer(l.Content.([]byte))
				controller.Store(descriptor.Key, vs)
				return vs.refer, vs.mutex, nil
			} else {
				return nilRefer, nil, ErrStateTypeMismatch
			}
		case *state[T]:
			return l.refer, l.mutex, nil
		default:
			return nilRefer, nil, ErrStateTypeMismatch
		}
	}
}

//func RegisterPaState[T any](controller Controller, descriptor ParallelizeStateDescriptor[T]) (T, error) {
//	var nilV T
//	if load, ok := controller.Load(descriptor.Key); !ok {
//		vs := &parallelizeState[T]{
//			refer:           map[int]*T{},
//			serializer:   descriptor.Serializer,
//			deserializer: descriptor.Deserializer,
//		}
//		v := descriptor.Initializer()
//		*vs.refer = v
//		controller.Store(descriptor.Key, vs)
//		return v, nil
//	} else {
//		switch l := load.(type) {
//		case mirrorState:
//			if l.Type == NonParallelizeState {
//				vs := &state[T]{
//					refer:           new(T),
//					serializer:   descriptor.Serializer,
//					deserializer: descriptor.Deserializer}
//				v := descriptor.Deserializer(l.Content.([]byte))
//				*vs.refer = v
//				controller.Store(descriptor.Key, vs)
//				return v, nil
//			} else {
//				return nilV, ErrStateTypeMismatch
//			}
//		case *state[T]:
//			return *l.refer, nil
//		default:
//			return nilV, ErrStateTypeMismatch
//		}
//	}
//}
