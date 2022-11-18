package watermark

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"math"
	"time"
)

// timestampAndWatermarkOperator will be set event time and advance watermark
type timestampAndWatermarkOperator[T any] struct {
	BaseOperator[T, any, T]
	watermarkGenerator GeneratorFn[T]
	timestampAssigner  TimestampAssignerFn[T]

	autoWatermarkInterval time.Duration
	timerService          *TimerService[struct{}]
	elementCollector      element.Collector[T]
	watermarkCollector    *collector[T]
}

func (o *timestampAndWatermarkOperator[T]) Open(ctx Context, elementCollector element.Collector[T]) error {
	if err := o.BaseOperator.Open(ctx, elementCollector); err != nil {
		return err
	}
	o.elementCollector = elementCollector
	o.watermarkCollector = &collector[T]{c: elementCollector}
	o.timerService = GetTimerService[struct{}](ctx, "timer-service", o)
	o.timerService.RegisterProcessingTimeTimer(Timer[struct{}]{Timestamp: time.Now().UnixMilli() + int64(o.autoWatermarkInterval/time.Millisecond)})
	return nil
}

func (o *timestampAndWatermarkOperator[T]) ProcessEvent(event *element.Event[T]) {
	timestamp := o.timestampAssigner(event.Value)
	event.Timestamp = timestamp
	o.elementCollector.EmitEvent(event)
	o.watermarkGenerator.OnEvent(event.Value, timestamp, o.watermarkCollector)
}

func (o *timestampAndWatermarkOperator[T]) OnProcessingTime(_ Timer[struct{}]) {
	o.watermarkGenerator.OnPeriodicEmit(o.watermarkCollector)
	o.timerService.RegisterProcessingTimeTimer(Timer[struct{}]{
		Timestamp: o.timerService.CurrentProcessingTimestamp() + int64(o.autoWatermarkInterval/time.Millisecond)})
}

func (o *timestampAndWatermarkOperator[T]) OnEventTime(_ Timer[struct{}]) {}

func (o *timestampAndWatermarkOperator[T]) ProcessWatermarkTimestamp(watermarkTimestamp int64) {
	if watermarkTimestamp == math.MaxInt64 {
		o.watermarkCollector.EmitWatermarkTimestamp(watermarkTimestamp)
	}
}

func Apply[T any](upstream stream.Stream[T],
	generatorFn GeneratorFn[T],
	timestampAssignerFn TimestampAssignerFn[T],
	autoWatermarkInterval time.Duration,
	name string, applyFns ...stream.WithOperatorStreamOptions[T, any, T]) (*stream.OperatorStream[T, any, T], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.Operator = OneInputOperatorToNormal[T, T](&timestampAndWatermarkOperator[T]{
		watermarkGenerator:    generatorFn,
		timestampAssigner:     timestampAssignerFn,
		autoWatermarkInterval: autoWatermarkInterval,
	})
	return stream.ApplyOneInput(upstream, options)
}
