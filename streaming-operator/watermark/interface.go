package watermark

import "github.com/RuiFG/streaming/streaming-core/element"

type Collector interface {
	EmitWatermarkTimestamp(watermarkTimestamp int64)
	MarkIdle()
	MarkActive()
}

type collector[T any] struct {
	c                         element.Collector[T]
	currentWatermarkTimestamp int64
	idle                      bool
}

func (c *collector[T]) EmitWatermarkTimestamp(watermarkTimestamp int64) {
	if watermarkTimestamp <= c.currentWatermarkTimestamp {
		return
	}
	c.currentWatermarkTimestamp = watermarkTimestamp
	c.MarkActive()
	c.c.EmitWatermark(&element.Watermark[T]{
		Timestamp: c.currentWatermarkTimestamp,
	})
}

func (c *collector[T]) MarkIdle() {
	if !c.idle {
		c.idle = true
		c.c.EmitWatermarkStatus(&element.WatermarkStatus[T]{StatusType: element.IdleWatermarkStatus})
	}
}

func (c *collector[T]) MarkActive() {
	if c.idle {
		c.idle = false
		c.c.EmitWatermarkStatus(&element.WatermarkStatus[T]{StatusType: element.IdleWatermarkStatus})
	}
}

type GeneratorFn[T any] interface {
	OnEvent(value T, timestamp int64, watermarkCollector Collector)
	OnPeriodicEmit(watermarkCollector Collector)
}

type TimestampAssignerFn[T any] func(value T) int64
