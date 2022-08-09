package store

import (
	"fmt"
)

var (
	ErrStateTypeMismatch = fmt.Errorf("state type error")
)

func MValueState[T any](manager Controller, descriptor ValueStateDescriptor[T]) (*ValueState[T], error) {
	if load, ok := manager.Load(descriptor.Key); !ok {
		vs := &ValueState[T]{}
		manager.Store(descriptor.Key, vs)
		return vs, nil
	} else {
		switch l := load.(type) {
		case mirrorState:
			if l.StateType == ValueType {
				vs := &ValueState[T]{
					v:            descriptor.Deserializer(l.Bytes),
					serializer:   descriptor.Serializer,
					deserializer: descriptor.Deserializer}
				manager.Store(descriptor.Key, vs)
				return vs, nil
			} else {
				return nil, ErrStateTypeMismatch
			}
		case *ValueState[T]:
			return l, nil
		default:
			return nil, ErrStateTypeMismatch
		}
	}
}

func MMapState[K comparable, V any](manager Controller, descriptor MapStateDescriptor[K, V]) (*MapState[K, V], error) {
	if load, ok := manager.Load(descriptor.Key); !ok {
		vs := &MapState[K, V]{}
		manager.Store(descriptor.Key, vs)
		return vs, nil
	} else {
		switch l := load.(type) {
		case mirrorState:
			if l.StateType == ValueType {
				vs := &MapState[K, V]{
					mm:           descriptor.Deserializer(l.Bytes),
					serializer:   descriptor.Serializer,
					deserializer: descriptor.Deserializer}
				manager.Store(descriptor.Key, vs)
				return vs, nil
			} else {
				return nil, ErrStateTypeMismatch
			}
		case *MapState[K, V]:
			return l, nil
		default:
			return nil, ErrStateTypeMismatch
		}
	}
}

//func AggregatingState[IN, OUT any](ctx athena.StreamContext, descriptor athena.Id, aggregator athena.Aggregator[IN, OUT]) (athena.AggregatingState[IN, OUT], error) {
//	var ni OUT
//	var aState athena.AggregatingState[IN, OUT]
//	if load, ok := ctx.Load(descriptor); !ok {
//		aState = NewAggregatingState[IN, OUT](ni, aggregator)
//	} else {
//		if aState, ok = load.(athena.AggregatingState[IN, OUT]); !ok {
//			return nil, ErrStateTypeMismatch
//		}
//	}
//	aState = NewAggregatingState[IN, OUT](aState.Value(), aggregator)
//	ctx.Store(descriptor, aState)
//	return aState, nil
//}
