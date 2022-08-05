package component

import (
	_c "context"
	"streaming/streaming-core/lib/barrier"
	element2 "streaming/streaming-core/lib/element"
	"streaming/streaming-core/lib/log"
	"streaming/streaming-core/lib/service"
	"streaming/streaming-core/lib/store"
)

type Context interface {
	_c.Context
	Logger() log.Logger
	Store() store.Manager
	TimeScheduler() service.TimeScheduler
}

type Source[OUT any] interface {
	barrier.Listener
	Open(ctx Context, collector element2.Collector[OUT]) error
	Close() error
}

type Operator[IN1, IN2 any, OUT any] interface {
	barrier.Listener
	Open(ctx Context, collector element2.Collector[OUT]) error
	Close() error

	ProcessEvent1(event *element2.Event[IN1])
	ProcessWatermark1(watermark *element2.Watermark[IN1])
	ProcessEvent2(event *element2.Event[IN2])
	ProcessWatermark2(watermark *element2.Watermark[IN2])
}

type Sink[IN any] interface {
	barrier.Listener
	Open(ctx Context) error
	Close() error

	ProcessEvent(event *element2.Event[IN])
	ProcessWatermark(watermark *element2.Watermark[IN])
}

type NewSource[T any] func() Source[T]
type NewOperator[IN1, IN2, OUT any] func() Operator[IN1, IN2, OUT]
type NewSink[T any] func() Sink[T]

// --------------------------default--------------------------------------

type Rich interface {
	Open(ctx Context) error
	Close() error
}

type Default[IN, OUT any] struct {
	Rich      Rich
	Ctx       Context
	Collector element2.Collector[OUT]
}

func (r *Default[IN, OUT]) Open(ctx Context, collector element2.Collector[OUT]) error {
	r.Collector = collector
	r.Ctx = ctx
	if r.Rich != nil {
		if err := r.Rich.Open(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Default[IN1, OUT]) Close() error {
	if r.Rich != nil {
		return r.Rich.Close()
	} else {
		return nil
	}
}
