package element

import "time"

type Watermark[_ any] struct {
	Meta
	//time is created event time
	Time time.Time `json:"time"`
}

func (w *Watermark[_]) GetMeta() Meta {
	return w.Meta
}

func (w *Watermark[_]) Type() Type {
	return WatermarkElement
}

func (w *Watermark[_]) AsEvent() *Event[_] {
	panic("implement me")
}

func (w *Watermark[_]) AsWatermark() *Watermark[_] {
	return w
}

func (w *Watermark[_]) AsBarrier() *Barrier[_] {
	//TODO implement me
	panic("implement me")
}
