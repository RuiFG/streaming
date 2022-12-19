package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"math"
)

type collector[T any] struct {
	element.Emit
}

func (c *collector[T]) EmitEvent(event *element.Event[T]) {
	c.Emit(event)
}

func (c *collector[T]) EmitWatermark(watermark element.Watermark) {
	c.Emit(watermark)
}

func (c *collector[T]) EmitWatermarkStatus(watermarkStatus element.WatermarkStatus) {
	c.Emit(watermarkStatus)
}

type oneInputOperator[IN, OUT any] struct {
	OneInputOperator[IN, OUT]
	currentWatermark element.Watermark
}

func (o *oneInputOperator[IN, OUT]) Open(ctx Context, emit element.Emit) error {
	o.currentWatermark = math.MinInt64
	return o.OneInputOperator.Open(ctx, &collector[OUT]{emit})
}

func (o *oneInputOperator[IN, OUT]) Close() error {
	return o.OneInputOperator.Close()
}

func (o *oneInputOperator[IN, OUT]) ProcessElement(e element.Element, _ int) {
	switch value := e.(type) {
	case *element.Event[IN]:
		o.OneInputOperator.ProcessEvent(value)
	case element.Watermark:
		o.OneInputOperator.ProcessWatermark(value)
	case element.WatermarkStatus:
		o.OneInputOperator.ProcessWatermarkStatus(value)
	}
}

type twoInputOperator[IN1, IN2, OUT any] struct {
	TwoInputOperator[IN1, IN2, OUT]
	combineWatermark *CombineWatermark
}

func (o *twoInputOperator[IN1, IN2, OUT]) Open(ctx Context, emit element.Emit) error {
	o.combineWatermark = NewCombineWatermark(2)
	return o.TwoInputOperator.Open(ctx, &collector[OUT]{emit})
}

func (o *twoInputOperator[IN1, IN2, OUT]) Close() error {
	return o.TwoInputOperator.Close()
}

func (o *twoInputOperator[IN1, IN2, OUT]) ProcessElement(e element.Element, index int) {
	if index == 0 {
		switch value := e.(type) {
		case *element.Event[IN1]:
			o.TwoInputOperator.ProcessEvent1(value)
		case element.Watermark:
			o.ProcessWatermark1(value)
		case element.WatermarkStatus:
			o.ProcessWatermarkStatus1(value)
		}
	} else if index == 1 {
		switch value := e.(type) {
		case *element.Event[IN2]:
			o.TwoInputOperator.ProcessEvent2(value)
		case element.Watermark:
			o.ProcessWatermark2(value)
		case element.WatermarkStatus:
			o.ProcessWatermarkStatus2(value)
		}
	}

}

func (o *twoInputOperator[IN1, IN2, OUT]) ProcessWatermark1(watermark element.Watermark) {
	o.processWatermark(watermark, 1)
}

func (o *twoInputOperator[IN1, IN2, OUT]) ProcessWatermark2(watermark element.Watermark) {
	o.processWatermark(watermark, 2)
}

func (o *twoInputOperator[IN1, IN2, OUT]) processWatermark(watermark element.Watermark, input int) {
	if o.combineWatermark.UpdateWatermark(watermark, input) {
		o.TwoInputOperator.ProcessWatermark(o.combineWatermark.GetCombinedWatermark())
	}
}

func (o *twoInputOperator[IN1, IN2, OUT]) ProcessWatermarkStatus1(watermarkStatus element.WatermarkStatus) {
	o.processWatermarkStatus(watermarkStatus, 1)
}

func (o *twoInputOperator[IN1, IN2, OUT]) ProcessWatermarkStatus2(watermarkStatus element.WatermarkStatus) {
	o.processWatermarkStatus(watermarkStatus, 2)
}

func (o *twoInputOperator[IN1, IN2, OUT]) processWatermarkStatus(watermarkStatus element.WatermarkStatus, input int) {
	wasIdle := o.combineWatermark.IsIdle()
	if o.combineWatermark.UpdateIdle(watermarkStatus == element.IdleWatermarkStatus, input) {
		o.ProcessWatermark(o.combineWatermark.GetCombinedWatermark())
	}
	if wasIdle != o.combineWatermark.IsIdle() {
		o.TwoInputOperator.ProcessWatermarkStatus(watermarkStatus)
	}
}

func OneInputOperatorToOperator[IN, OUT any](operator OneInputOperator[IN, OUT]) Operator {
	return &oneInputOperator[IN, OUT]{OneInputOperator: operator}
}

func TwoInputOperatorToOperator[IN1, IN2, OUT any](operator TwoInputOperator[IN1, IN2, OUT]) Operator {
	return &twoInputOperator[IN1, IN2, OUT]{TwoInputOperator: operator}
}
