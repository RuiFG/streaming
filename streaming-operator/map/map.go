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
	collector element.Collector[OUT]
	Rich      Rich
	Fn        Fn[IN, OUT]
}

func (m *operator[IN, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	if err := m.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	m.collector = collector
	if m.Rich != nil {
		return m.Rich.Open(ctx)
	} else {
		return nil
	}
}
func (m *operator[IN, OUT]) Close() error {
	if err := m.BaseOperator.Close(); err != nil {
		return err
	}
	if m.Rich != nil {
		return m.Rich.Close()
	} else {
		return nil
	}
}

func (m *operator[IN, OUT]) ProcessEvent1(event *element.Event[IN]) {
	m.collector.EmitEvent(&element.Event[OUT]{Value: m.Fn(event.Value), Timestamp: event.Timestamp})
}

func Apply[IN, OUT any](upstreams []stream.Stream[IN], fn Fn[IN, OUT], name string, applyFns ...stream.WithOperatorStreamOptions[IN, any, OUT]) (*stream.OperatorStream[IN, any, OUT], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() Operator[IN, any, OUT] {
		return &operator[IN, OUT]{Fn: fn}
	}
	return stream.ApplyOneInput(upstreams, options)
}

func ApplyRich[IN, OUT any](upstreams []stream.Stream[IN], richFn RichFn[IN, OUT], name string, applyFns ...stream.WithOperatorStreamOptions[IN, any, OUT]) (*stream.OperatorStream[IN, any, OUT], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() Operator[IN, any, OUT] {
		return &operator[IN, OUT]{
			Fn:               richFn.Apply,
			BaseRichOperator: BaseRichOperator[IN, any, OUT]{Rich: richFn},
		}
	}
	return stream.ApplyOneInput[IN, OUT](upstreams, options)
}
