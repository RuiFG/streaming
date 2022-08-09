package task

import (
	"github.com/RuiFG/streaming/streaming-core/barrier"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type QOS uint

const (
	AtMostOnce QOS = iota
	AtLeastOnce
	ExactlyOnce
)

type Options struct {
	QOS
	NameSuffix         string
	BarrierSignalChan  chan element.Signal
	BarrierTriggerChan chan element.BarrierType

	InputCount   int
	OutputCount  int
	StoreManager store.Manager
}

func (o Options) Name() string {
	return "." + o.NameSuffix
}

type Task interface {
	Name() string
	//Daemon  Running is life cycle
	Daemon() error
	Running() bool

	barrier.Trigger
	barrier.Listener
}
