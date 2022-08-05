package stream

import (
	"streaming/streaming-core/lib/element"
	task2 "streaming/streaming-core/lib/task"
	"sync"
)

type OneInputOperatorStream[IN, OUT any] struct {
	task2.OperatorOptions[IN, any, OUT]
	Env                 *Env
	downstreamInitFnMap map[string]downstreamInitFn[OUT]
	once                *sync.Once
	task                *task2.OperatorTask[IN, any, OUT]
}

func (o *OneInputOperatorStream[IN, OUT]) addDownstream(name string, downstreamInitFn downstreamInitFn[OUT]) {
	o.downstreamInitFnMap[name] = downstreamInitFn
}

func (o *OneInputOperatorStream[IN, OUT]) Init(upstream string) (element.EmitNext[IN], []task2.Task, error) {
	var (
		tasks   []task2.Task
		initErr error
	)
	o.once.Do(func() {
		var (
			emitNextSlice      []element.EmitNext[OUT]
			allDownstreamTasks []task2.Task
		)
		for _, initFn := range o.downstreamInitFnMap {
			if emitNext, downstreamTasks, err := initFn(o.Name()); err != nil {
				initErr = err
				return
			} else {
				emitNextSlice = append(emitNextSlice, emitNext)
				if downstreamTasks != nil {
					allDownstreamTasks = append(allDownstreamTasks, downstreamTasks...)
				}
			}
			o.task = task2.NewOperatorTask[IN, any, OUT](o.Env.ctx, o.OperatorOptions, nil)
			tasks = append(allDownstreamTasks, o.task)
		}
	})
	return o.task.Emit1, tasks, initErr
}
