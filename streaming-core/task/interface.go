package task

import (
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
)

// Data like element.NormalElement
type Data any

type Emit func(Data)

type internalData struct {
	index int
	eob   Data
}

type Options struct {
	Name              string
	Operator          operator.NormalOperator
	BarrierSignalChan chan<- Signal
	Logger            *zap.Logger
	Scope             tally.Scope

	//sink DataEmit is nil
	DataEmit Emit

	BufferSize   int
	StoreManager store.Manager
}
