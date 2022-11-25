package _map

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type options[IN, OUT any] struct {
	rich     Rich
	function Fn[IN, OUT]
}

type WithOptions[IN, OUT any] func(options *options[IN, OUT]) error

func WithRich[IN, OUT any](rich Rich) WithOptions[IN, OUT] {
	return func(opts *options[IN, OUT]) error {
		opts.rich = rich
		return nil
	}
}

func WithFn[IN, OUT any](fn Fn[IN, OUT]) WithOptions[IN, OUT] {
	return func(opts *options[IN, OUT]) error {
		opts.function = fn
		return nil
	}
}

type Fn[IN any, OUT any] func(event IN) OUT

type operator[IN any, OUT any] struct {
	BaseRichOperator[IN, any, OUT]
	Fn Fn[IN, OUT]
}

func (m *operator[IN, OUT]) ProcessEvent(event *element.Event[IN]) {
	m.Collector.EmitEvent(&element.Event[OUT]{Value: m.Fn(event.Value), Timestamp: event.Timestamp})
}

func Apply[IN, OUT any](upstream stream.Stream[IN], name string, withOptions ...WithOptions[IN, OUT]) (stream.Stream[OUT], error) {
	o := &options[IN, OUT]{}
	for _, withOptionsFn := range withOptions {
		if err := withOptionsFn(o); err != nil {
			return nil, err
		}
	}
	if o.function == nil {
		return nil, nil
	}
	var normalOperator NormalOperator
	if o.rich == nil {
		normalOperator = OneInputOperatorToNormal[IN, OUT](&operator[IN, OUT]{Fn: o.function})
	} else {
		normalOperator = OneInputOperatorToNormal[IN, OUT](
			&operator[IN, OUT]{
				Fn:               o.function,
				BaseRichOperator: BaseRichOperator[IN, any, OUT]{Rich: o.rich},
			})
	}
	return stream.ApplyOneInput[IN, OUT](upstream, stream.OperatorStreamOptions{
		Name:     name,
		Operator: normalOperator,
	})
}
