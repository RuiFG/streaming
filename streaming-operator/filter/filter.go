package filter

import (
	"errors"
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
	BaseOperator[T, any, T]
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
		return nil, errors.New("function can't be nil")
	}
	return stream.ApplyOneInput[T, T](upstream, stream.OperatorStreamOptions{
		Name: name,
		Operator: OneInputOperatorToOperator[T, T](&operator[T]{
			BaseOperator: BaseOperator[T, any, T]{Rich: o.rich},
			Fn:           o.function,
		}),
	})
}
