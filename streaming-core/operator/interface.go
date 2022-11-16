package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type CheckpointListener interface {
	NotifyCheckpointCome(checkpointId int64)
	NotifyCheckpointComplete(checkpointId int64)
	NotifyCheckpointCancel(checkpointId int64)
}

type Context interface {
	Logger() log.Logger
	Store() store.Controller
	TimerManager() *TimerManager
	//Call will call func that are mutually exclusive
	Call(func())
}

// Operator is the operator's basic interface.
type Operator[IN1, IN2, OUT any] interface {
	CheckpointListener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	ProcessEvent1(event *element.Event[IN1])
	ProcessEvent2(event *element.Event[IN2])
	ProcessWatermarkTimestamp(watermarkTimestamp int64)
	ProcessWatermarkStatus(watermarkStatus element.WatermarkStatusType)
}

type NewOperator[IN1, IN2, OUT any] func() Operator[IN1, IN2, OUT]

type Source[OUT any] interface {
	CheckpointListener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	Run()
}

type NewSource[OUT any] func() Source[OUT]

type Sink[IN any] interface {
	CheckpointListener
	Open(ctx Context) error
	Close() error

	ProcessEvent(event *element.Event[IN])
	ProcessWatermarkTimestamp(watermarkTimestamp int64)
}

type NewSink[IN any] func() Sink[IN]

type Rich interface {
	Open(ctx Context) error
	Close() error
}
