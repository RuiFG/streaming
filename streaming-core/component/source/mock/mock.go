package mock

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/source"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
	"time"
)

type Fn[T any] func() T

type source[OUT any] struct {
	Default[OUT]
	ctx       component.Context
	collector element.Collector[OUT]
	doneChan  chan struct{}
	interval  time.Duration
	Fn[OUT]
}

func (s *source[OUT]) Open(ctx component.Context, collector element.Collector[OUT]) error {
	s.ctx = ctx
	s.collector = collector
	s.doneChan = make(chan struct{})
	return nil
}

func (s *source[OUT]) Run() {
	for {
		select {
		case <-s.doneChan:
			return
		default:
			s.collector.EmitValue(s.Fn())
			time.Sleep(s.interval)
		}
	}
}

func (s *source[OUT]) Close() error {
	close(s.doneChan)
	return nil
}

func FormSource[OUT any](env *stream.Env, GeneratorFn Fn[OUT], interval time.Duration, nameSuffix string) (*stream.SourceStream[OUT], error) {
	return stream.FormSource(env, task.SourceOptions[OUT]{
		Options: task.Options{
			NameSuffix: nameSuffix,
		},
		New: func() component.Source[OUT] {
			return &source[OUT]{
				Fn: GeneratorFn, interval: interval}
		}})

}
