package sink

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type ProcessEventFn[IN any] func(IN)

type ProcessWatermarkFn func(watermark element.Watermark)

type sink[IN any] struct {
	ProcessEventFn[IN]
	ProcessWatermarkFn
}

func (s *sink[IN]) NotifyCheckpointCome(checkpointId int64) {

}

func (s *sink[IN]) NotifyCheckpointComplete(checkpointId int64) {

}

func (s *sink[IN]) NotifyCheckpointCancel(checkpointId int64) {

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

func ToSink[IN any](upstream stream.Stream[IN], name string, processEventFn ProcessEventFn[IN], processWatermarkFn ProcessWatermarkFn) error {
	return stream.ToSink[IN](upstream, stream.SinkStreamOptions[IN]{
		Name: name,
		Sink: &sink[IN]{
			ProcessEventFn:     processEventFn,
			ProcessWatermarkFn: processWatermarkFn,
		},
	})
}
