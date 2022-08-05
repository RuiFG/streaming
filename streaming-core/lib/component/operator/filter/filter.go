package filter

import (
	"streaming/streaming-core/lib/component"
	. "streaming/streaming-core/lib/component/operator"
	"streaming/streaming-core/lib/element"
	stream2 "streaming/streaming-core/lib/stream"
	"streaming/streaming-core/lib/task"
)

type Fn[T any] func(event T) bool

type RichFn[T any] interface {
	component.Rich
	Apply(event T) bool
}

type operator[IN any] struct {
	Default[IN, any, IN]
	Fn Fn[IN]
}

func (f *operator[IN]) ProcessEvent1(event *element.Event[IN]) {
	if !f.Fn(event.Value) {
		f.Default.Collector.EmitValue(event.Value)
	}
}

func Of[T any](upstream stream2.Stream[T], fn Fn[T], nameSuffix string) *stream2.OneInputOperatorStream[T, T] {
	outputStream := &stream2.OneInputOperatorStream[T, T]{
		Env: upstream.Env(),
		OperatorOptions: task.OperatorOptions[T, any, T]{New: func() component.Operator[T, any, T] {
			return &operator[T]{Fn: fn}
		}, NameSuffix: nameSuffix},
	}
	upstream.AddDownstream(outputStream.Name(), outputStream.Init)
	return outputStream
}

func RichOf[T any](upstream stream2.Stream[T], richFn RichFn[T], nameSuffix string) *stream2.OneInputOperatorStream[T, T] {
	outputStream := &stream2.OneInputOperatorStream[T, T]{
		OperatorOptions: task.OperatorOptions[T, any, T]{New: func() component.Operator[T, any, T] {
			return &operator[T]{
				Default: Default[T, any, T]{component.Default[any, T]{Rich: richFn}},
				Fn:      richFn.Apply,
			}
		}, NameSuffix: nameSuffix,
		},
	}
	upstream.AddDownstream(outputStream.Name(), outputStream.Init)
	return outputStream
}
