package filter

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/operator"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
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

func Apply[T any](upstreams []stream.Stream[T], fn Fn[T], nameSuffix string) (*stream.OperatorStream[T, any, T], error) {
	return stream.ApplyOneInput(upstreams, task.OperatorOptions[T, any, T]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Operator[T, any, T] {
			return &operator[T]{Fn: fn}
		},
	})

}

func ApplyRich[T any](upstreams []stream.Stream[T], richFn RichFn[T], nameSuffix string) (*stream.OperatorStream[T, any, T], error) {
	return stream.ApplyOneInput(upstreams, task.OperatorOptions[T, any, T]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Operator[T, any, T] {
			return &operator[T]{
				Fn:      richFn.Apply,
				Default: Default[T, any, T]{Default: component.Default{Rich: richFn}},
			}
		},
	})
}
