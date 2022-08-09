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
	collector element.Collector[OUT]
	Fn        Fn[IN, OUT]
}

func (m *operator[IN, OUT]) ProcessEvent1(event element.Event[IN]) {
	m.Default.Collector.EmitValue(m.Fn(event.Value))
}

func Apply[IN, OUT any](upstreams []stream.Stream[IN], fn Fn[IN, OUT], nameSuffix string) (*stream.OperatorStream[IN, any, OUT], error) {
	return stream.ApplyOneInput(upstreams, task.OperatorOptions[IN, any, OUT]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Operator[IN, any, OUT] {
			return &operator[IN, OUT]{Fn: fn}
		}})
}

func ApplyRich[IN, OUT any](upstreams []stream.Stream[IN], richFn RichFn[IN, OUT], nameSuffix string) (*stream.OperatorStream[IN, any, OUT], error) {
	return stream.ApplyOneInput[IN, OUT](upstreams, task.OperatorOptions[IN, any, OUT]{
		New: func() component.Operator[IN, any, OUT] {
			return &operator[IN, OUT]{
				Default: Default[IN, any, OUT]{Default: component.Default{Rich: richFn}},
				Fn:      richFn.Map,
			}
		}, Options: task.Options{NameSuffix: nameSuffix}},
	)
}
