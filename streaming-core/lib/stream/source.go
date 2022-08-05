package stream

import (
	"streaming/streaming-core/lib/element"
	task2 "streaming/streaming-core/lib/task"
)

type SourceStream[OUT any] struct {
	task2.SourceOptions[OUT]
	_env                *Env
	downstreamInitFnMap map[string]downstreamInitFn[OUT]
}

func (s *SourceStream[T]) addDownstream(name string, downstreamInitFn downstreamInitFn[T]) {
	s.downstreamInitFnMap[name] = downstreamInitFn
}

func (s *SourceStream[OUT]) env() *Env {
	return s._env
}

func (s *SourceStream[T]) init() ([]task2.Task, error) {
	var (
		emitNextSlice []element.EmitNext[T]
		tasks         []task2.Task
	)

	for _, fn := range s.downstreamInitFnMap {
		if emitNext, downstreamTasks, err := fn(s.Name()); err != nil {
			return nil, err
		} else {
			emitNextSlice = append(emitNextSlice, emitNext)
			tasks = append(tasks, downstreamTasks...)
		}
	}
	return append(tasks, task2.NewSourceTask[T](s._env.ctx, s.SourceOptions)), nil
}
