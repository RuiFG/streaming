package store

type ValueSerializer[T any] func(T any) []byte
type ValueDeserializer[T any] func([]byte) T

type ValueStateDescriptor[T any] struct {
	Key          string
	Serializer   ValueSerializer[T]
	Deserializer ValueDeserializer[T]
}

type ValueState[T any] struct {
	v            T
	serializer   ValueSerializer[T]
	deserializer ValueDeserializer[T]
}

func (v *ValueState[T]) Initialized() bool { return true }

func (v *ValueState[T]) Clear() {
	var ni T
	v.v = ni
}

func (v *ValueState[T]) Mirror() MirrorState {
	return MirrorState{_type: ValueType, _bytes: v.serializer(v.v)}
}

func (v *ValueState[T]) Value() T {
	return v.v
}

func (v *ValueState[T]) Update(value T) {
	v.v = value
}
