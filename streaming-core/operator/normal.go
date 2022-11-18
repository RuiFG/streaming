package operator

import (
	"encoding/binary"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"math"
	"sync"
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

type OneInputNormalOperator[IN, OUT any] struct {
	OneInputOperator[IN, OUT]
	currentWatermark *element.Watermark
}

func (o *OneInputNormalOperator[IN, OUT]) Open(ctx Context, emit element.Emit) (err error) {
	o.currentWatermark, _, err = store.RegisterOrGet(ctx.Store(), store.StateDescriptor[element.Watermark]{
		Key: "current-watermark",
		Initializer: func() element.Watermark {
			return math.MinInt64
		},
		Serializer: func(watermark element.Watermark) []byte {
			var buf = make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(watermark))
			return buf
		},
		Deserializer: func(buf []byte) element.Watermark {
			return element.Watermark(binary.BigEndian.Uint64(buf))
		},
	})
	if err != nil {
		return errors.WithMessage(err, "failed to register current watermark state.")
	}

	return o.OneInputOperator.Open(ctx, &collector[OUT]{emit})
}

func (o *OneInputNormalOperator[IN, OUT]) Close() error {
	return o.OneInputOperator.Close()
}

func (o *OneInputNormalOperator[IN, OUT]) ProcessElement(normalElement element.NormalElement, _ int) {
	switch e := normalElement.(type) {
	case *element.Event[IN]:
		o.OneInputOperator.ProcessEvent(e)
	case element.Watermark:
		o.OneInputOperator.ProcessWatermark(e)
	case element.WatermarkStatus:
		o.OneInputOperator.ProcessWatermarkStatus(e)
	}
}

type TwoInputNormalOperator[IN1, IN2, OUT any] struct {
	TwoInputOperator[IN1, IN2, OUT]
	combineWatermarkMutex *sync.RWMutex
	combineWatermark      *CombineWatermark
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) Open(ctx Context, emit element.Emit) error {
	combineWatermark, mutex, err := store.RegisterOrGet(ctx.Store(), NewCombineWatermarkStateDescriptor("combine-watermark", 2))
	if err != nil {
		return errors.WithMessage(err, "failed to register combine watermark.")
	}
	o.combineWatermark = combineWatermark
	o.combineWatermarkMutex = mutex
	return o.TwoInputOperator.Open(ctx, &collector[OUT]{emit})
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) Close() error {
	return o.TwoInputOperator.Close()
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) ProcessElement(normalElement element.NormalElement, index int) {
	if index == 0 {
		switch e := normalElement.(type) {
		case *element.Event[IN1]:
			o.TwoInputOperator.ProcessEvent1(e)
		case element.Watermark:
			o.ProcessWatermark1(e)
		case element.WatermarkStatus:
			o.ProcessWatermarkStatus1(e)
		}
	} else if index == 1 {
		switch e := normalElement.(type) {
		case *element.Event[IN2]:
			o.TwoInputOperator.ProcessEvent2(e)
		case element.Watermark:
			o.ProcessWatermark2(e)
		case element.WatermarkStatus:
			o.ProcessWatermarkStatus2(e)
		}
	}

}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) ProcessWatermark1(watermark element.Watermark) {
	o.processWatermark(watermark, 1)
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) ProcessWatermark2(watermark element.Watermark) {
	o.processWatermark(watermark, 2)
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) processWatermark(watermark element.Watermark, input int) {
	if o.combineWatermark.UpdateWatermark(watermark, input) {
		o.TwoInputOperator.ProcessWatermark(o.combineWatermark.GetCombinedWatermark())
	}
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) ProcessWatermarkStatus1(watermarkStatus element.WatermarkStatus) {
	o.processWatermarkStatus(watermarkStatus, 1)
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) ProcessWatermarkStatus2(watermarkStatus element.WatermarkStatus) {
	o.processWatermarkStatus(watermarkStatus, 2)
}

func (o *TwoInputNormalOperator[IN1, IN2, OUT]) processWatermarkStatus(watermarkStatus element.WatermarkStatus, input int) {
	wasIdle := o.combineWatermark.IsIdle()
	if o.combineWatermark.UpdateIdle(watermarkStatus == element.IdleWatermarkStatus, input) {
		o.ProcessWatermark(o.combineWatermark.GetCombinedWatermark())
	}
	if wasIdle != o.combineWatermark.IsIdle() {
		o.TwoInputOperator.ProcessWatermarkStatus(watermarkStatus)
	}
}

func OneInputOperatorToNormal[IN, OUT any](operator OneInputOperator[IN, OUT]) *OneInputNormalOperator[IN, OUT] {
	return &OneInputNormalOperator[IN, OUT]{OneInputOperator: operator}
}

func TwoInputOperatorToNormal[IN1, IN2, OUT any](operator TwoInputOperator[IN1, IN2, OUT]) *TwoInputNormalOperator[IN1, IN2, OUT] {
	return &TwoInputNormalOperator[IN1, IN2, OUT]{TwoInputOperator: operator}
}
