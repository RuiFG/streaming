package operator

import (
	"streaming/streaming-core/lib/barrier"
	"streaming/streaming-core/lib/component"
	element2 "streaming/streaming-core/lib/element"
)

type Default[IN1, IN2, OUT any] struct {
	component.Default[any, OUT]
}

func (o *Default[IN1, IN2, OUT]) ProcessEvent1(_ *element2.Event[IN1]) {}

func (o *Default[IN1, IN2, OUT]) ProcessWatermark1(watermark *element2.Watermark[IN1]) {
	o.Collector.EmitWatermark(&element2.Watermark[OUT]{Time: watermark.Time})
}

func (o *Default[IN1, IN2, OUT]) ProcessEvent2(_ *element2.Event[IN2]) {}

func (o *Default[IN1, IN2, OUT]) ProcessWatermark2(watermark *element2.Watermark[IN2]) {
	o.Collector.EmitWatermark(&element2.Watermark[OUT]{Time: watermark.Time})
}

func (o *Default[IN1, IN2, OUT]) NotifyComplete(detail barrier.Detail) {}

func (o *Default[IN1, IN2, OUT]) NotifyCancel(detail barrier.Detail) {}
