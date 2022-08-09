package store

type MapSerializer[K comparable, V any] func(map[K]V) []byte
type MapDeserializer[K comparable, V any] func([]byte) map[K]V

type MapStateDescriptor[K comparable, V any] struct {
	Key          string
	Serializer   MapSerializer[K, V]
	Deserializer MapDeserializer[K, V]
}

type MapState[K comparable, V any] struct {
	mm           map[K]V
	serializer   MapSerializer[K, V]
	deserializer MapDeserializer[K, V]
}

func (m *MapState[K, V]) Initialized() bool {
	return true
}

func (m *MapState[K, V]) Clear() {
	m.mm = map[K]V{}
}

func (m *MapState[K, V]) mirror() mirrorState {
	mm := map[K]V{}
	for k, v := range m.mm {
		mm[k] = v
	}
	return mirrorState{StateType: MapType, Bytes: m.serializer(mm)}
}

func (m *MapState[K, V]) Load(k K) (V, bool) {
	var ni V
	if load, ok := m.mm[k]; !ok {
		return ni, ok
	} else {
		return load, ok
	}
}

func (m *MapState[K, V]) Store(k K, v V) {
	m.mm[k] = v
}

func (m *MapState[K, V]) Delete(k K) {
	delete(m.mm, k)
}

func (m *MapState[K, V]) Range(fn func(key K, value V) bool) {
	for k, v := range m.mm {
		if !fn(k, v) {
			return
		}
	}
}

func (m *MapState[K, V]) Size() int {
	return len(m.mm)
}
