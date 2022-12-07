package element

type Event[T any] struct {
	//keep in mind that it is not thread-safe when you modify
	Value        T
	Timestamp    int64
	HasTimestamp bool
}

func Copy[T, N any](event *Event[T], value N) *Event[N] {
	return &Event[N]{
		Value:     value,
		Timestamp: event.Timestamp,
	}
}
