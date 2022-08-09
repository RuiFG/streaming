package flat_map

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/operator"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
)

type Fn[IN, OUT any] func(event IN) []OUT

type RichFn[IN, OUT any] interface {
	component.Rich
	Apply(event IN) []OUT
}

type operator[IN any, OUT any] struct {
	Default[IN, any, OUT]
	Fn Fn[IN, OUT]
}

func (m *operator[IN, OUT]) ProcessEvent1(event element.Event[IN]) {
	values := m.Fn(event.Value)
	for _, e := range values {
		m.Default.Collector.EmitValue(e)
	}
}

func Apply[IN, OUT any](upstreams []stream.Stream[IN], fn Fn[IN, OUT], nameSuffix string) (*stream.OperatorStream[IN, any, OUT], error) {
	return stream.ApplyOneInput(upstreams, task.OperatorOptions[IN, any, OUT]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Operator[IN, any, OUT] {
			return &operator[IN, OUT]{Fn: fn}
		},
	})

}

func ApplyRich[IN, OUT any](upstreams []stream.Stream[IN], richFn RichFn[IN, OUT], nameSuffix string) (*stream.OperatorStream[IN, any, OUT], error) {
	return stream.ApplyOneInput(upstreams, task.OperatorOptions[IN, any, OUT]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Operator[IN, any, OUT] {
			return &operator[IN, OUT]{
				Fn:      richFn.Apply,
				Default: Default[IN, any, OUT]{Default: component.Default{Rich: richFn}},
			}
		},
	})
}
