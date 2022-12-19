package operator

import (
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
)

type Context interface {
	Logger() *zap.Logger
	Scope() tally.Scope
	Store() store.Controller
	TimerManager() *TimerManager
	//Exec will call func that are mutually exclusive
	Exec(func()) *executor.Executor
}

type CheckpointListener interface {
	NotifyCheckpointCome(checkpointId int64)
	NotifyCheckpointComplete(checkpointId int64)
	NotifyCheckpointCancel(checkpointId int64)
}

type Operator interface {
	CheckpointListener
	Open(ctx Context, emit element.Emit) error
	Close() error
	ProcessElement(element element.Element, index int)
}

type OneInputOperator[IN, OUT any] interface {
	CheckpointListener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	ProcessEvent(event *element.Event[IN])
	ProcessWatermark(watermark element.Watermark)
	ProcessWatermarkStatus(watermarkStatus element.WatermarkStatus)
}

type TwoInputOperator[IN1, IN2, OUT any] interface {
	CheckpointListener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	ProcessEvent1(event *element.Event[IN1])
	ProcessEvent2(event *element.Event[IN2])
	ProcessWatermark(watermark element.Watermark)
	ProcessWatermarkStatus(watermarkStatus element.WatermarkStatus)
}

type Source[OUT any] interface {
	CheckpointListener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	Run()
}

type Sink[IN any] interface {
	CheckpointListener
	Open(ctx Context) error
	Close() error

	ProcessEvent(event *element.Event[IN])
	ProcessWatermark(watermark element.Watermark)
}

type Rich interface {
	Open(ctx Context) error
	Close() error
}
