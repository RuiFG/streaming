package operator

import (
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/pkg/errors"
)

type SourceOperatorWrap[OUT any] struct {
	Source[OUT]
	collector element.Collector[OUT]
}

func (o *SourceOperatorWrap[OUT]) ProcessEvent1(_ *element.Event[any]) {}

func (o *SourceOperatorWrap[OUT]) ProcessEvent2(_ *element.Event[any]) {}

func (o *SourceOperatorWrap[OUT]) ProcessWatermark(_ element.Watermark) {}

func (o *SourceOperatorWrap[OUT]) ProcessWatermarkStatus(_ element.WatermarkStatus) {}

func (o *SourceOperatorWrap[OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	o.collector = collector
	if err := o.Source.Open(ctx, collector); err != nil {
		return errors.WithMessage(err, "failed to open source operator")
	}
	go func() {
		if err := safe.Run(func() error {
			o.Source.Run()
			return nil
		}); err == nil {
			return
		}
		ctx.Logger().Warnf("source operator exited unexpectedly, restarting...")
	}()
	return nil
}

func (o *SourceOperatorWrap[IN]) Close() error { return o.Source.Close() }

func (o *SourceOperatorWrap[OUT]) NotifyCheckpointCome(checkpointId int64) {
	o.Source.NotifyCheckpointCome(checkpointId)
}

func (o *SourceOperatorWrap[OUT]) NotifyCheckpointComplete(checkpointId int64) {
	o.Source.NotifyCheckpointComplete(checkpointId)
}
func (o *SourceOperatorWrap[OUT]) NotifyCheckpointCancel(checkpointId int64) {
	o.Source.NotifyCheckpointCancel(checkpointId)
}

type SinkOperatorWrap[IN any] struct {
	Sink[IN]
}

func (s *SinkOperatorWrap[IN]) Open(ctx Context, _ element.Collector[any]) error {
	//sink operator collector is nil
	if err := s.Sink.Open(ctx); err != nil {
		return errors.WithMessage(err, "failed to open sink operator")
	}
	return nil
}

func (s *SinkOperatorWrap[IN]) Close() error { return s.Sink.Close() }

func (s *SinkOperatorWrap[IN]) ProcessEvent1(event *element.Event[IN]) { s.Sink.ProcessEvent(event) }

func (s *SinkOperatorWrap[IN]) ProcessEvent2(_ *element.Event[any]) {}

func (s *SinkOperatorWrap[IN]) ProcessWatermark(_ element.Watermark) {}

func (s *SinkOperatorWrap[IN]) ProcessWatermarkStatus(_ element.WatermarkStatus) {}

func (s *SinkOperatorWrap[IN]) NotifyCheckpointCome(checkpointId int64) {
	s.Sink.NotifyCheckpointCome(checkpointId)
}

func (s *SinkOperatorWrap[IN]) NotifyCheckpointComplete(checkpointId int64) {
	s.Sink.NotifyCheckpointComplete(checkpointId)
}

func (s *SinkOperatorWrap[IN]) NotifyCheckpointCancel(checkpointId int64) {
	s.Sink.NotifyCheckpointCancel(checkpointId)
}
