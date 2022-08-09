package element

type Meta struct {
	Partition uint
	Upstream  string
}

type Event[T any] struct {
	Meta
	Value T
}

func (e *Event[T]) GetMeta() Meta {
	return e.Meta
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

func (e *Event[T]) AsBarrier() *Barrier[T] {
	panic("implement me")
}

type Collector[T any] struct {
	Emit Emit[T]
	Meta Meta
}

func (c Collector[T]) EmitValue(value T) {
	c.Emit(&Event[T]{Meta: c.Meta, Value: value})
}

func (c Collector[T]) EmitWatermark(watermark *Watermark[T]) {
	c.Emit(watermark)
}
