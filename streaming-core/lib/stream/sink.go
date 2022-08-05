package stream

import (
	"streaming/streaming-core/lib/element"
	task2 "streaming/streaming-core/lib/task"
	"sync"
)

type SinkStream[IN any] struct {
	task2.SinkOptions[IN]
	_env *Env
	once *sync.Once
	task *task2.SinkTask[IN]
}

func (o *SinkStream[IN]) init(upstream string) (element.EmitNext[IN], []task2.Task, error) {
	var (
		tasks []task2.Task
	)
	o.once.Do(func() {
		o.task = task2.NewSinkTask[IN](o._env.ctx, o.SinkOptions)
		tasks = []task2.Task{}
	})
	return o.task.Emit, tasks, nil

}
