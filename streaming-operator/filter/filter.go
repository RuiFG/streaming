package filter

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type Fn[T any] func(event T) bool

type RichFn[T any] interface {
	Rich
	Apply(event T) bool
}

type operator[T any] struct {
	BaseRichOperator[T, any, T]
	Fn Fn[T]
}

func (o *operator[IN]) ProcessEvent1(event *element.Event[IN]) {
	if !o.Fn(event.Value) {
		o.Collector.EmitEvent(event)
	}
}

func Apply[T any](upstreams []stream.Stream[T], fn Fn[T], name string, applyFns ...stream.WithOperatorStreamOptions[T, any, T]) (*stream.OperatorStream[T, any, T], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() Operator[T, any, T] {
		return &operator[T]{Fn: fn}
	}
	return stream.ApplyOneInput(upstreams, options)

}

func ApplyRich[T any](upstreams []stream.Stream[T], richFn RichFn[T], name string, applyFns ...stream.WithOperatorStreamOptions[T, any, T]) (*stream.OperatorStream[T, any, T], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() Operator[T, any, T] {
		return &operator[T]{
			BaseRichOperator: BaseRichOperator[T, any, T]{Rich: richFn},
			Fn:               richFn.Apply,
		}
	}
	return stream.ApplyOneInput(upstreams, options)
}
