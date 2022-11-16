package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
)

// BaseOperator is part of the operator's functions to help developers save some work
type BaseOperator[IN1, IN2, OUT any] struct {
	Ctx          Context
	Collector    element.Collector[OUT]
	TimerManager *TimerManager
}

func (o *BaseOperator[IN1, IN2, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	o.Ctx = ctx
	o.Collector = collector
	o.TimerManager = ctx.TimerManager()
	return nil
}

func (o *BaseOperator[IN1, IN2, OUT]) Close() error {
	return nil
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessEvent1(event *element.Event[IN1]) {
	panic("base operator can't process event")
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessEvent2(event *element.Event[IN2]) {
	panic("base operator can't process event")
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessWatermarkStatus(watermarkStatus element.WatermarkStatusType) {
	o.Collector.EmitWatermarkStatus(&element.WatermarkStatus[OUT]{
		StatusType: watermarkStatus,
	})
}

func (o *BaseOperator[IN1, IN2, OUT]) ProcessWatermarkTimestamp(watermarkTimestamp int64) {
	o.TimerManager.advanceWatermarkTimestamp(watermarkTimestamp)
	o.Collector.EmitWatermark(&element.Watermark[OUT]{
		Timestamp: watermarkTimestamp,
	})
}

func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointCome(_ int64)     {}
func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointComplete(_ int64) {}
func (o *BaseOperator[IN1, IN2, OUT]) NotifyCheckpointCancel(_ int64)   {}

// BaseRichOperator included Rich and BaseOperator
type BaseRichOperator[IN1, IN2, OUT any] struct {
	Rich Rich
	BaseOperator[IN1, IN2, OUT]
}

func (o *BaseRichOperator[IN1, IN2, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	if err := o.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	if o.Rich != nil {
		return o.Rich.Open(ctx)
	} else {
		return nil
	}
}

func (o *BaseRichOperator[IN1, IN2, OUT]) Close() error {
	if err := o.BaseOperator.Close(); err != nil {
		return err
	}
	if o.Rich != nil {
		return o.Rich.Close()
	} else {
		return nil
	}
}
