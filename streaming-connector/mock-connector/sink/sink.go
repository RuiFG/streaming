package sink

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type ProcessEventFn[IN any] func(IN)

type ProcessWatermarkFn func(timestamp int64)

type sink[IN any] struct {
	BaseOperator[IN, any, any]
	ProcessEventFn[IN]
	ProcessWatermarkFn
}

func (s *sink[IN]) Open(ctx Context) error {
	return nil
}

func (s *sink[IN]) ProcessEvent(event *element.Event[IN]) {
	s.ProcessEventFn(event.Value)
}

func (s *sink[IN]) ProcessWatermarkTimestamp(watermarkTimestamp int64) {
	s.ProcessWatermarkFn(watermarkTimestamp)
}

func ToSink[IN any](upstreams []stream.Stream[IN], processEventFn ProcessEventFn[IN], processWatermarkFn ProcessWatermarkFn, name string, applyFns ...stream.WithSinkStreamOptions[IN]) error {
	options := stream.ApplyWithSinkStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() operator.Sink[IN] {
		return &sink[IN]{
			ProcessEventFn:     processEventFn,
			ProcessWatermarkFn: processWatermarkFn,
		}
	}
	return stream.ToSink[IN](upstreams, options)
}
