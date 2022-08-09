package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
)

type emitsOutputHandler[OUT any] struct {
	emits []element.Emit[OUT]
}

func (o *emitsOutputHandler[OUT]) OnElement(e element.Element[OUT]) {
	for _, emit := range o.emits {
		emit(e)
	}
}

type SourceStream[OUT any] struct {
	task.SourceOptions[OUT]
	env                 *Env
	downstreamInitFnMap map[string]downstreamInitFn[OUT]
}

func (s *SourceStream[T]) Env() *Env {
	return s.env
}

func (s *SourceStream[T]) addDownstream(name string, downstreamInitFn downstreamInitFn[T]) {
	s.downstreamInitFnMap[name] = downstreamInitFn
}

func (s *SourceStream[T]) addUpstream(name string) {
	panic("can't implement me")
}

func (s *SourceStream[T]) Init() (task.Task, []task.Task, error) {
	var (
		emits []element.Emit[T]
		tasks []task.Task
	)
	for _, fn := range s.downstreamInitFnMap {
		if emitNext, downstreamTasks, err := fn(s.Name()); err != nil {
			return nil, nil, err
		} else {
			emits = append(emits, emitNext)
			tasks = append(tasks, downstreamTasks...)
		}
	}
	s.SourceOptions.OutputCount = len(s.downstreamInitFnMap)
	s.SourceOptions.OutputHandler = &emitsOutputHandler[T]{emits: emits}
	return task.NewSourceTask[T](s.SourceOptions), tasks, nil
}

func FormSource[OUT any](env *Env, sourceOptions task.SourceOptions[OUT]) (*SourceStream[OUT], error) {
	sourceOptions.Options.QOS = env.qos
	sourceOptions.Options.BarrierSignalChan = env.barrierSignalChan
	sourceOptions.Options.BarrierTriggerChan = env.barrierTriggerChan
	sourceOptions.Options.StoreManager = store.NewNonPersistenceManager(sourceOptions.Name(), env.storeBackend)
	sourceStream := &SourceStream[OUT]{
		env:                 env,
		SourceOptions:       sourceOptions,
		downstreamInitFnMap: map[string]downstreamInitFn[OUT]{}}
	env.addSourceInit(sourceStream.Init)
	return sourceStream, nil

}
