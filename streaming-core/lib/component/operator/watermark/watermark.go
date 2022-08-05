package watermark

import (
	"streaming/streaming-core/lib/component"
	. "streaming/streaming-core/lib/component/operator"
	element2 "streaming/streaming-core/lib/element"
	"time"
)

type GeneratorFn[T any] interface {
	OnEvent(value T, collector func(watermark *element2.Watermark[T]))
	OnPeriodicEmit(collector func(watermark *element2.Watermark[T]))
}

type TimestampAssignerFn[T any] func(value T) time.Time

type StrategyFn[T any] interface {
	AutoWatermarkInterval() time.Duration
	CreateGenerator() GeneratorFn[T]
	CreateTimestampAssigner() TimestampAssignerFn[T]
}

type operator[T any] struct {
	Default[T, any, T]
	strategy           StrategyFn[T]
	watermarkGenerator GeneratorFn[T]
	timestampAssigner  TimestampAssignerFn[T]
}

func (o *operator[T]) OnProcessingTime(time.Time) {
	o.watermarkGenerator.OnPeriodicEmit(o.Default.Collector.EmitWatermark)
	o.Ctx.TimeScheduler().RegisterTicker(o.strategy.AutoWatermarkInterval(), o)
}

func (o *operator[T]) Open(ctx component.Context, collector element2.Collector[T]) error {
	if err := o.Default.Open(ctx, collector); err != nil {
		return err
	}
	o.watermarkGenerator = o.strategy.CreateGenerator()
	o.timestampAssigner = o.strategy.CreateTimestampAssigner()
	o.Ctx.TimeScheduler().RegisterTicker(o.strategy.AutoWatermarkInterval(), o)
	return nil
}

func (o *operator[T]) ProcessEvent1(event *element2.Event[T]) {
	o.Default.Collector.EmitValue(event.Value)
	o.watermarkGenerator.OnEvent(event.Value, o.Default.Collector.EmitWatermark)
}

func (o *operator[T]) ProcessWatermark1(_ *element2.Watermark[T]) {}
