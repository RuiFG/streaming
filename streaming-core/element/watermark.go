package element

import "time"

type Watermark[T any] struct {
	Meta
	//time is created event time
	Time time.Time `json:"time"`
}

func (w Watermark[T]) GetMeta() Meta {
	return w.Meta
}

func (w Watermark[T]) Type() Type {
	return WatermarkElement
}

func (w Watermark[T]) AsEvent() Event[T] {
	panic("implement me")
}

func (w Watermark[T]) AsWatermark() Watermark[T] {
	return w
}

func (w Watermark[T]) AsBarrier() Barrier[T] {
	//TODO implement me
	panic("implement me")
}
