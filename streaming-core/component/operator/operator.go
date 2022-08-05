package operator

import (
	"github.com/RuiFG/streaming/streaming-core/barrier"
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
)

type Default[IN1, IN2, OUT any] struct {
	component.Default[any, OUT]
}

func (o *Default[IN1, IN2, OUT]) ProcessEvent1(_ *element.Event[IN1]) {}

func (o *Default[IN1, IN2, OUT]) ProcessWatermark1(watermark *element.Watermark[IN1]) {
	o.Collector.EmitWatermark(&element.Watermark[OUT]{Time: watermark.Time})
}

func (o *Default[IN1, IN2, OUT]) ProcessEvent2(_ *element.Event[IN2]) {}

func (o *Default[IN1, IN2, OUT]) ProcessWatermark2(watermark *element.Watermark[IN2]) {
	o.Collector.EmitWatermark(&element.Watermark[OUT]{Time: watermark.Time})
}

func (o *Default[IN1, IN2, OUT]) NotifyComplete(detail barrier.Detail) {}

func (o *Default[IN1, IN2, OUT]) NotifyCancel(detail barrier.Detail) {}
