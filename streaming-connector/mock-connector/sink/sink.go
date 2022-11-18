package sink

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type ProcessEventFn[IN any] func(IN)

type ProcessWatermarkFn func(watermark element.Watermark)

type sink[IN any] struct {
	CheckpointListener
	ProcessEventFn[IN]
	ProcessWatermarkFn
}

func (s *sink[IN]) Open(_ Context) error {
	return nil
}

func (s *sink[IN]) Close() error {
	return nil
}

func (s *sink[IN]) ProcessEvent(event *element.Event[IN]) {
	s.ProcessEventFn(event.Value)
}

func (s *sink[IN]) ProcessWatermark(watermark element.Watermark) {
	s.ProcessWatermarkFn(watermark)
}

func ToSink[IN any](upstream stream.Stream[IN], processEventFn ProcessEventFn[IN], processWatermarkFn ProcessWatermarkFn, name string, applyFns ...stream.WithSinkStreamOptions[IN]) error {
	options := stream.ApplyWithSinkStreamOptionsFns(applyFns)
	options.Name = name
	options.Sink = &sink[IN]{
		ProcessEventFn:     processEventFn,
		ProcessWatermarkFn: processWatermarkFn,
	}
	return stream.ToSink[IN](upstream, options)
}
