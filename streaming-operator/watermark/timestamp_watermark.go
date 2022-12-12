package watermark

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/pkg/errors"
	"math"
	"time"
)

type options[T any] struct {
	generatorFn           GeneratorFn[T]
	timestampAssignerFn   TimestampAssignerFn[T]
	autoWatermarkInterval time.Duration
}

type WithOptions[T any] func(options *options[T]) error

func WithBoundedOutOfOrderlinessWatermarkGenerator[T any](outOfOrderlinessMillisecond time.Duration) WithOptions[T] {
	return func(options *options[T]) error {
		if outOfOrderlinessMillisecond <= time.Millisecond && outOfOrderlinessMillisecond != 0 {
			return errors.Errorf("outOfOrderlinessMillisecond should be more than milliseconds.")
		}
		options.generatorFn = &boundedOutOfOrderlinessWatermarkGeneratorFn[T]{
			maxTimestamp:                int64(math.MinInt64 + outOfOrderlinessMillisecond/time.Millisecond + 1),
			outOfOrderlinessMillisecond: int64(outOfOrderlinessMillisecond / time.Millisecond),
		}
		return nil
	}
}

func WithNoWatermarksGenerator[T any]() WithOptions[T] {
	return func(options *options[T]) error {
		options.generatorFn = &noWatermarksGeneratorFn[T]{}
		return nil
	}
}

func WithTimestampAssigner[T any](fn TimestampAssignerFn[T]) WithOptions[T] {
	return func(options *options[T]) error {
		options.timestampAssignerFn = fn
		return nil
	}
}

func WithAutoWatermarkInterval[T any](duration time.Duration) WithOptions[T] {
	return func(options *options[T]) error {
		if duration <= 0 {
			return errors.Errorf("duration should be greater than 0")
		}
		options.autoWatermarkInterval = duration
		return nil
	}
}

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

func (o *timestampAndWatermarkOperator[T]) Open(ctx Context, elementCollector element.Collector[T]) (err error) {
	if err = o.BaseOperator.Open(ctx, elementCollector); err != nil {
		return err
	}
	o.elementCollector = elementCollector
	o.watermarkCollector = &collector[T]{c: elementCollector}
	if o.timerService, err = GetTimerService[struct{}](ctx, "timer-service", o); err != nil {
		return errors.WithMessage(err, "failed to open timestampAndWatermarkOperator")
	}
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
	name string, withOptionsFns ...WithOptions[T]) (stream.Stream[T], error) {
	o := &options[T]{
		generatorFn:           nil,
		timestampAssignerFn:   nil,
		autoWatermarkInterval: 60 * time.Second,
	}
	for _, withOptionsFn := range withOptionsFns {
		if err := withOptionsFn(o); err != nil {
			return nil, err
		}
	}
	if o.generatorFn == nil {
		return nil, errors.Errorf("generatorFn can't be nil")
	}
	if o.timestampAssignerFn == nil {
		return nil, errors.Errorf("timestampAssignerFn can't be nil")
	}
	if o.autoWatermarkInterval <= 0 {
		return nil, errors.Errorf("autoWatermarkInterval should be greater than 0")
	}

	return stream.ApplyOneInput[T, T](upstream, stream.OperatorStreamOptions{
		Name: name,
		Operator: OneInputOperatorToNormal[T, T](&timestampAndWatermarkOperator[T]{
			watermarkGenerator:    o.generatorFn,
			timestampAssigner:     o.timestampAssignerFn,
			autoWatermarkInterval: o.autoWatermarkInterval,
		}),
	})
}
