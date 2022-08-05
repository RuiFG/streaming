package _map

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/operator"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
)

type Fn[IN any, OUT any] func(event IN) OUT

type RichFn[IN any, OUT any] interface {
	component.Rich
	Map(event IN) OUT
}

type operator[IN any, OUT any] struct {
	Default[IN, any, OUT]
	Fn Fn[IN, OUT]
}

func (m *operator[IN, OUT]) ProcessEvent1(event *element.Event[IN]) {
	m.Default.Collector.EmitValue(m.Fn(event.Value))
}

func Of[OIN, OUT any](upstream stream.Stream[OIN], fn Fn[OIN, OUT], nameSuffix string) *stream.OneInputOperatorStream[OIN, OUT] {
	outputStream := &stream.OneInputOperatorStream[OIN, OUT]{
		Env: upstream.Env(),
		OperatorOptions: task.OperatorOptions[OIN, any, OUT]{New: func() component.Operator[OIN, any, OUT] {
			return &operator[OIN, OUT]{Fn: fn}
		}, NameSuffix: nameSuffix},
	}
	upstream.AddDownstream(outputStream.Name(), outputStream.Init)
	return outputStream
}

func RichOf[OIN, OUT any](upstream stream.Stream[OIN], richFn RichFn[OIN, OUT], nameSuffix string) *stream.OneInputOperatorStream[OIN, OUT] {
	outputStream := &stream.OneInputOperatorStream[OIN, OUT]{
		OperatorOptions: task.OperatorOptions[OIN, any, OUT]{New: func() component.Operator[OIN, any, OUT] {
			return &operator[OIN, OUT]{
				Default: Default[OIN, any, OUT]{component.Default[any, OUT]{Rich: richFn}},
				Fn:      richFn.Map,
			}
		}, NameSuffix: nameSuffix,
		},
	}
	upstream.AddDownstream(outputStream.Name(), outputStream.Init)
	return outputStream
}
