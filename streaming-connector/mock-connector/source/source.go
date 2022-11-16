package source

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"time"
)

type Fn[T any] func() T

type mockSource[OUT any] struct {
	BaseOperator[any, any, OUT]
	ctx       operator.Context
	collector element.Collector[OUT]
	doneChan  chan struct{}
	interval  time.Duration
	number    int
	Fn[OUT]
}

func (s *mockSource[OUT]) Open(ctx operator.Context, collector element.Collector[OUT]) error {
	s.ctx = ctx
	s.collector = collector
	s.doneChan = make(chan struct{})
	return nil
}

func (s *mockSource[OUT]) Run() {
	for {
		select {
		case <-s.doneChan:
			return
		default:
			for i := 0; i < s.number; i++ {
				s.collector.EmitEvent(&element.Event[OUT]{
					Value:     s.Fn(),
					Timestamp: 0,
				})
			}
			time.Sleep(s.interval)

		}
	}
}

func (s *mockSource[OUT]) Close() error {
	close(s.doneChan)
	return nil
}

func FormSource[OUT any](env *stream.Env, GeneratorFn Fn[OUT], interval time.Duration, number int, name string, applyFns ...stream.WithSourceStreamOptions[OUT]) (*stream.SourceStream[OUT], error) {
	options := stream.ApplyWithSourceStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() operator.Source[OUT] {
		return &mockSource[OUT]{
			Fn: GeneratorFn, interval: interval, number: number}
	}
	return stream.FormSource(env, options)

}
