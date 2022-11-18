package _map

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type Fn[IN any, OUT any] func(event IN) OUT

type RichFn[IN any, OUT any] interface {
	Rich
	Apply(event IN) OUT
}

type operator[IN any, OUT any] struct {
	BaseRichOperator[IN, any, OUT]
	Fn Fn[IN, OUT]
}

func (m *operator[IN, OUT]) ProcessEvent(event *element.Event[IN]) {
	m.Collector.EmitEvent(&element.Event[OUT]{Value: m.Fn(event.Value), Timestamp: event.Timestamp})
}

func Apply[IN, OUT any](upstream stream.Stream[IN], fn Fn[IN, OUT], name string, applyFns ...stream.WithOperatorStreamOptions[IN, any, OUT]) (*stream.OperatorStream[IN, any, OUT], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.Operator = OneInputOperatorToNormal[IN, OUT](&operator[IN, OUT]{Fn: fn})
	return stream.ApplyOneInput(upstream, options)
}

func ApplyRich[IN, OUT any](upstream stream.Stream[IN], richFn RichFn[IN, OUT], name string, applyFns ...stream.WithOperatorStreamOptions[IN, any, OUT]) (*stream.OperatorStream[IN, any, OUT], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.Operator = OneInputOperatorToNormal[IN, OUT](
		&operator[IN, OUT]{
			Fn:               richFn.Apply,
			BaseRichOperator: BaseRichOperator[IN, any, OUT]{Rich: richFn},
		})
	return stream.ApplyOneInput[IN, OUT](upstream, options)
}
