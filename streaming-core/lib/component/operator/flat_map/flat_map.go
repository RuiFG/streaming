package flat_map

import (
	"streaming/streaming-core/lib/component"
	. "streaming/streaming-core/lib/component/operator"
	"streaming/streaming-core/lib/element"
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

func (m *operator[IN, OUT]) ProcessEvent1(event *element.Event[IN]) {
	values := m.Fn(event.Value)
	for _, e := range values {
		m.Default.Collector.EmitValue(e)
	}
}
