package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type SinkStream[IN any] struct {
	task.SinkOptions[IN]
	_env *Env
	once *sync.Once
	task *task.SinkTask[IN]
}

func (o *SinkStream[IN]) init(upstream string) (element.EmitNext[IN], []task.Task, error) {
	var (
		tasks []task.Task
	)
	o.once.Do(func() {
		o.task = task.NewSinkTask[IN](o._env.ctx, o.SinkOptions)
		tasks = []task.Task{}
	})
	return o.task.Emit, tasks, nil

}
