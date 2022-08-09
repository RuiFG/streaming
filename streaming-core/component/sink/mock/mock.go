package mock

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/sink"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
	"time"
)

type ProcessEventFn[IN any] func(IN)
type ProcessWatermarkFn func(time time.Time)
type sink[IN any] struct {
	Default[IN]
	ProcessEventFn[IN]
	ProcessWatermarkFn
}

func (s *sink[IN]) ProcessEvent(event element.Event[IN]) {
	s.ProcessEventFn(event.Value)
}

func (s *sink[IN]) ProcessWatermark(time element.Watermark[IN]) {
	s.ProcessWatermarkFn(time.Time)
}

func ToSink[IN any](upstreams []stream.Stream[IN], processEventFn ProcessEventFn[IN], processWatermarkFn ProcessWatermarkFn, nameSuffix string) error {
	return stream.ToSink[IN](upstreams, task.SinkOptions[IN]{
		Options: task.Options{
			NameSuffix: nameSuffix,
		}, New: func() component.Sink[IN] {
			return &sink[IN]{
				ProcessEventFn:     processEventFn,
				ProcessWatermarkFn: processWatermarkFn,
			}
		}})
}
