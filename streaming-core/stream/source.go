package stream

import (
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type SourceStreamOptions[OUT any] struct {
	Name   string
	Source operator.Source[OUT]
}

type SourceStream[OUT any] struct {
	OperatorStream[OUT]
}

func (s *SourceStream[T]) Init() (*task.Task, []*task.Task, error) {
	s.init()
	return s.task, s.chainTask, nil
}

func FormSource[OUT any](env *Environment, sourceOptions SourceStreamOptions[OUT]) (Stream[OUT], error) {
	sourceStream := &SourceStream[OUT]{
		OperatorStream: OperatorStream[OUT]{
			options: OperatorStreamOptions{
				Name: sourceOptions.Name,
				Operator: operator.OneInputOperatorToNormal[any, OUT](&operator.SourceOperatorWrap[OUT]{
					Source: sourceOptions.Source,
				}),
			},
			env:                 env,
			downstreamInitFnMap: map[string]downstreamInitFn{},
			once:                &sync.Once{},
		},
	}
	env.addSourceInit(sourceStream.Init)
	return sourceStream, nil

}
