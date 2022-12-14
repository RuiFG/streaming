package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
)

// BaseOperator is part of the operator's functions to help developers save some work
// it impl OneInputOperator and TwoInputOperator
type BaseOperator[IN1, IN2, OUT any] struct {
	Ctx          Context
	Collector    element.Collector[OUT]
	TimerManager *TimerManager
	Rich         Rich
}

func (o *BaseOperator[IN1, IN2, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	o.Ctx = ctx
	o.Collector = collector
	o.TimerManager = ctx.TimerManager()
	if o.Rich != nil {
		return o.Rich.Open(ctx)
	} else {
		return nil
	}
}

func (o *BaseOperator[IN1, IN2, OUT]) Close() error {
	if o.Rich != nil {
		return o.Rich.Close()
	} else {
		return nil
	}
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessEvent(_ *element.Event[IN1]) {
	panic("base operator can't process event")
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessEvent1(_ *element.Event[IN1]) {
	panic("base operator can't process event")
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessEvent2(_ *element.Event[IN2]) {
	panic("base operator can't process event")
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessWatermark(watermark element.Watermark) {
	o.TimerManager.advanceWatermarkTimestamp(int64(watermark))
	o.Collector.EmitWatermark(watermark)
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessWatermarkStatus(watermarkStatus element.WatermarkStatus) {
	o.Collector.EmitWatermarkStatus(watermarkStatus)
}

func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointCome(_ int64)     {}
func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointComplete(_ int64) {}
func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointCancel(_ int64)   {}
