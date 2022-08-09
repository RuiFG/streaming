package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type SinkStream[IN any] struct {
	task.SinkOptions[IN]
	env         *Env
	once        *sync.Once
	task        *task.SinkTask[IN]
	upstreamMap map[string]struct{}
}

func (o *SinkStream[IN]) Env() *Env {
	return o.env
}

func (o *SinkStream[IN]) addDownstream(string, downstreamInitFn[IN]) {
	panic("can't implement me")
}

func (o *SinkStream[IN]) addUpstream(name string) {
	o.upstreamMap[name] = struct{}{}
}

func (o *SinkStream[IN]) init(upstream string) (element.Emit[IN], []task.Task, error) {
	var (
		tasks []task.Task
	)
	o.once.Do(func() {
		o.SinkOptions.InputCount = len(o.upstreamMap)
		o.task = task.NewSinkTask[IN](o.SinkOptions)
		tasks = []task.Task{o.task}
	})
	return o.task.Emit, tasks, nil

}

func ToSink[IN any](upstreams []Stream[IN], sinkOptions task.SinkOptions[IN]) error {
	var env *Env
	for _, upstream := range upstreams {
		if env == nil {
			env = upstream.Env()
		} else if env != upstream.Env() {
			return ErrMultipleEnv
		}
	}
	sinkOptions.Options.QOS = env.qos
	sinkOptions.Options.BarrierSignalChan = env.barrierSignalChan
	sinkOptions.Options.BarrierTriggerChan = env.barrierTriggerChan
	sinkOptions.Options.StoreManager = store.NewNonPersistenceManager(sinkOptions.Name(), env.storeBackend)
	sinkStream := &SinkStream[IN]{
		env:         env,
		once:        &sync.Once{},
		SinkOptions: sinkOptions,
		upstreamMap: map[string]struct{}{},
	}
	for _, upstream := range upstreams {
		upstream.addDownstream(sinkStream.Name(), sinkStream.init)
		sinkStream.addUpstream(upstream.Name())
	}
	return nil
}
