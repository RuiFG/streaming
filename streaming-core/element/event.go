package element

type Event[T any] struct {
	//keep in mind that it is not thread-safe when you modify
	Value        T
	Timestamp    int64
	HasTimestamp bool
}

func (e *Event[T]) Type() Type {
	return EventElement
}

func (e *Event[T]) AsEvent() *Event[T] {
	return e
}

func (e *Event[T]) AsWatermark() *Watermark[T] {
	panic("implement me")
}

func (e *Event[T]) AsWatermarkStatus() *WatermarkStatus[T] {
	//TODO implement me
	panic("implement me")
}

func Copy[T, N any](event *Event[T], value N) *Event[N] {
	return &Event[N]{
		Value:     value,
		Timestamp: event.Timestamp,
	}
}
