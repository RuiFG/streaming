package component

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/barrier"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type Context interface {
	_c.Context
	Logger() log.Logger
	Store() store.Controller
	TimeScheduler() service.TimeScheduler
}

type Source[OUT any] interface {
	barrier.Listener
	Open(ctx Context, collector element.Collector[OUT]) error
	Run()
	Close() error
}

type Operator[IN1, IN2 any, OUT any] interface {
	barrier.Listener
	Open(ctx Context, collector element.Collector[OUT]) error
	Close() error

	ProcessEvent1(event element.Event[IN1])
	ProcessWatermark1(watermark element.Watermark[IN1])
	ProcessEvent2(event element.Event[IN2])
	ProcessWatermark2(watermark element.Watermark[IN2])
}

type Sink[IN any] interface {
	barrier.Listener
	Open(ctx Context) error
	Close() error

	ProcessEvent(event element.Event[IN])
	ProcessWatermark(watermark element.Watermark[IN])
}

type NewSource[T any] func() Source[T]
type NewOperator[IN1, IN2, OUT any] func() Operator[IN1, IN2, OUT]
type NewSink[T any] func() Sink[T]

// --------------------------default--------------------------------------

type Rich interface {
	Open(ctx Context) error
	Close() error
}

type Default struct {
	Rich Rich
	Ctx  Context
}

func (r *Default) Open(ctx Context) error {
	r.Ctx = ctx
	if r.Rich != nil {
		if err := r.Rich.Open(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Default) Close() error {
	if r.Rich != nil {
		return r.Rich.Close()
	} else {
		return nil
	}
}
