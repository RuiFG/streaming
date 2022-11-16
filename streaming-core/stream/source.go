package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type SourceStreamOptions[OUT any] struct {
	Options
	New              operator.NewSource[OUT]
	ElementListeners []element.Listener[any, any, OUT]
}

type WithSourceStreamOptions[OUT any] func(options SourceStreamOptions[OUT]) SourceStreamOptions[OUT]

func ApplyWithSourceStreamOptionsFns[OUT any](applyFns []WithSourceStreamOptions[OUT]) SourceStreamOptions[OUT] {
	options := SourceStreamOptions[OUT]{Options: Options{ChannelSize: 1024}}
	for _, fn := range applyFns {
		options = fn(options)
	}
	return options
}

type SourceStream[OUT any] struct {
	OperatorStream[any, any, OUT]
}

func (s *SourceStream[T]) Init() (task.Task, []task.Task, error) {
	s.init()
	return s.task, s.downstreamTask, nil
}

func FormSource[OUT any](env *Env, sourceOptions SourceStreamOptions[OUT]) (*SourceStream[OUT], error) {
	sourceStream := &SourceStream[OUT]{
		OperatorStream[any, any, OUT]{
			options: OperatorStreamOptions[any, any, OUT]{
				Options: sourceOptions.Options,
				New: func() operator.Operator[any, any, OUT] {
					return &operator.SourceOperatorWrap[OUT]{
						Source: sourceOptions.New(),
					}
				},
				ElementListeners: sourceOptions.ElementListeners,
			},
			env:                 env,
			downstreamInitFnMap: map[string]downstreamInitFn[OUT]{},
			once:                &sync.Once{},
		},
	}
	env.addSourceInit(sourceStream.Init)
	return sourceStream, nil

}
