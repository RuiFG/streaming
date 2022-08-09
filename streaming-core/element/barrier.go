package element

type Barrier[T any] struct {
	Meta
	Detail
}

func (b Barrier[T]) GetMeta() Meta {
	return b.Meta
}

func (b Barrier[T]) Type() Type {
	return BarrierElement
}

func (b Barrier[T]) AsEvent() Event[T] {
	panic("implement me")
}

func (b Barrier[T]) AsWatermark() Watermark[T] {
	panic("implement me")
}

func (b Barrier[T]) AsBarrier() Barrier[T] {
	return b
}
