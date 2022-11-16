package task

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type QOS uint

const (
	AtMostOnce QOS = iota
	AtLeastOnce
	ExactlyOnce
)

type Options[IN1, IN2, OUT any] struct {
	Name               string
	QOS                QOS
	BarrierSignalChan  chan Signal
	BarrierTriggerChan chan BarrierType

	ElementListeners []element.Listener[IN1, IN2, OUT]
	//sink Emit is nil
	Emit Emit
	New  operator.NewOperator[IN1, IN2, OUT]

	InputCount   int
	OutputCount  int
	ChannelSize  int
	StoreManager store.Manager
}

type Task interface {
	Name() string
	//Daemon  Running is life cycle
	Daemon() error
	Running() bool

	BarrierTrigger
	BarrierListener
}
