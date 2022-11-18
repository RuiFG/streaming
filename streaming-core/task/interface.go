package task

import (
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type QOS uint

const (
	AtMostOnce QOS = iota
	AtLeastOnce
	ExactlyOnce
)

// Data like element.NormalElement
type Data any

type Emit func(Data)

type internalData struct {
	index int
	eob   Data
}

type Options struct {
	Name               string
	Operator           operator.NormalOperator
	QOS                QOS
	BarrierSignalChan  chan Signal
	BarrierTriggerChan chan BarrierType

	//sink EmitNext is nil
	EmitNext Emit

	InputCount   int
	OutputCount  int
	ChannelSize  int
	StoreManager store.Manager
}
