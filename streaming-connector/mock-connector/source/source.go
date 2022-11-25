package source

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"time"
)

type Fn[T any] func() T

type mockSource[OUT any] struct {
	ctx       operator.Context
	collector element.Collector[OUT]
	doneChan  chan struct{}
	interval  time.Duration
	number    int
	Fn[OUT]
}

func (s *mockSource[OUT]) NotifyCheckpointCome(checkpointId int64) {

}

func (s *mockSource[OUT]) NotifyCheckpointComplete(checkpointId int64) {

}

func (s *mockSource[OUT]) NotifyCheckpointCancel(checkpointId int64) {
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

func FormSource[OUT any](env *stream.Environment, name string, fn Fn[OUT], interval time.Duration, number int) (stream.Stream[OUT], error) {
	return stream.FormSource(env, stream.SourceStreamOptions[OUT]{
		Name: name,
		Source: &mockSource[OUT]{
			interval: interval,
			number:   number,
			Fn:       fn,
		},
	})

}
