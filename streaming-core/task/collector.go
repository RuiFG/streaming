package task

import (
	"github.com/RuiFG/streaming/streaming-core/element"
)

type ElementOrBarrier struct {
	Upstream string
	Value    any
}

type Emit func(ElementOrBarrier)

type collector[T any] struct {
	upstream string
	Emit     Emit
}

func (c *collector[T]) EmitEvent(event *element.Event[T]) {
	c.Emit(ElementOrBarrier{
		Upstream: c.upstream,
		Value:    event,
	})
}

func (c *collector[T]) EmitWatermark(watermark element.Watermark) {
	c.Emit(ElementOrBarrier{
		Upstream: c.upstream,
		Value:    watermark,
	})
}

func (c *collector[T]) EmitWatermarkStatus(watermarkStatus element.WatermarkStatus) {
	c.Emit(ElementOrBarrier{
		Upstream: c.upstream,
		Value:    watermarkStatus,
	})
}
