package filter

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type options[T any] struct {
	rich     Rich
	function Fn[T]
}

type WithOptions[T any] func(options *options[T]) error

func WithRich[T any](rich Rich) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.rich = rich
		return nil
	}
}

func WithFn[T any](fn Fn[T]) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.function = fn
		return nil
	}
}

type Fn[T any] func(event T) bool

type RichFn[T any] interface {
	Rich
	Apply(event T) bool
}

type operator[T any] struct {
	BaseRichOperator[T, any, T]
	Fn Fn[T]
}

func (o *operator[IN]) ProcessEvent(event *element.Event[IN]) {
	if !o.Fn(event.Value) {
		o.Collector.EmitEvent(event)
	}
}

func Apply[T any](upstream stream.Stream[T], name string, withOptions ...WithOptions[T]) (stream.Stream[T], error) {
	o := &options[T]{}
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
		normalOperator = OneInputOperatorToNormal[T, T](&operator[T]{Fn: o.function})
	} else {
		normalOperator = OneInputOperatorToNormal[T, T](
			&operator[T]{
				Fn:               o.function,
				BaseRichOperator: BaseRichOperator[T, any, T]{Rich: o.rich},
			})
	}
	return stream.ApplyOneInput[T, T](upstream, stream.OperatorStreamOptions{
		Name:     name,
		Operator: normalOperator,
	})
}
