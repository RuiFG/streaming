package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type OperatorStream[IN1, IN2, OUT any] struct {
	task.OperatorOptions[IN1, IN2, OUT]
	env                 *Env
	downstreamInitFnMap map[string]downstreamInitFn[OUT]
	upstreamMap         map[string]struct{}
	once                *sync.Once
	task                *task.OperatorTask[IN1, IN2, OUT]
}

func (o *OperatorStream[IN1, IN2, OUT]) Env() *Env {
	return o.env
}

func (o *OperatorStream[IN1, IN2, OUT]) addUpstream(name string) {
	o.upstreamMap[name] = struct{}{}
}

func (o *OperatorStream[IN1, IN2, OUT]) addDownstream(name string, downstreamInitFn downstreamInitFn[OUT]) {
	o.downstreamInitFnMap[name] = downstreamInitFn
}

func (o *OperatorStream[IN1, IN2, OUT]) Init1(upstream string) (element.Emit[IN1], []task.Task, error) {
	var (
		tasks   []task.Task
		initErr error
	)
	o.once.Do(func() {
		var (
			emits              []element.Emit[OUT]
			allDownstreamTasks []task.Task
		)
		for _, initFn := range o.downstreamInitFnMap {
			if emitNext, downstreamTasks, err := initFn(o.Name()); err != nil {
				initErr = err
				return
			} else {
				emits = append(emits, emitNext)
				if downstreamTasks != nil {
					allDownstreamTasks = append(allDownstreamTasks, downstreamTasks...)
				}
			}
			o.OperatorOptions.InputCount = len(o.upstreamMap)
			o.OperatorOptions.OutputCount = len(o.downstreamInitFnMap)
			o.OperatorOptions.OutputHandler = &emitsOutputHandler[OUT]{emits: emits}
			o.task = task.NewOperatorTask[IN1, IN2, OUT](o.OperatorOptions)
			tasks = append(allDownstreamTasks, o.task)
		}
	})
	return o.task.Emit1, tasks, initErr
}

func (o *OperatorStream[IN1, IN2, OUT]) Init2(upstream string) (element.Emit[IN2], []task.Task, error) {
	var (
		tasks   []task.Task
		initErr error
	)
	o.once.Do(func() {
		var (
			emits              []element.Emit[OUT]
			allDownstreamTasks []task.Task
		)
		for _, initFn := range o.downstreamInitFnMap {
			if emitNext, downstreamTasks, err := initFn(o.Name()); err != nil {
				initErr = err
				return
			} else {
				emits = append(emits, emitNext)
				if downstreamTasks != nil {
					allDownstreamTasks = append(allDownstreamTasks, downstreamTasks...)
				}
			}
			o.OperatorOptions.InputCount = len(o.upstreamMap)
			o.OperatorOptions.OutputCount = len(o.downstreamInitFnMap)
			o.OperatorOptions.OutputHandler = &emitsOutputHandler[OUT]{emits: emits}
			o.task = task.NewOperatorTask[IN1, IN2, OUT](o.OperatorOptions)
			tasks = append(allDownstreamTasks, o.task)
		}
	})
	return o.task.Emit2, tasks, initErr
}

func ApplyOneInput[IN, OUT any](upstreams []Stream[IN], operatorOptions task.OperatorOptions[IN, any, OUT]) (*OperatorStream[IN, any, OUT], error) {
	var env *Env
	for _, upstream := range upstreams {
		if env == nil {
			env = upstream.Env()
		} else if env != upstream.Env() {
			return nil, ErrMultipleEnv
		}
	}
	operatorOptions.Options.QOS = env.qos
	operatorOptions.Options.BarrierSignalChan = env.barrierSignalChan
	operatorOptions.Options.BarrierTriggerChan = env.barrierTriggerChan
	operatorOptions.Options.StoreManager = store.NewNonPersistenceManager(operatorOptions.Name(), env.storeBackend)
	outputStream := &OperatorStream[IN, any, OUT]{
		env:                 env,
		once:                &sync.Once{},
		OperatorOptions:     operatorOptions,
		upstreamMap:         map[string]struct{}{},
		downstreamInitFnMap: map[string]downstreamInitFn[OUT]{},
	}
	for _, upstream := range upstreams {
		upstream.addDownstream(outputStream.Name(), outputStream.Init1)
		outputStream.addUpstream(upstream.Name())
	}
	return outputStream, nil
}

func ApplyTwoInput[IN1, IN2, OUT any](leftUpstream Stream[IN1], rightUpstream Stream[IN2], operatorOptions task.OperatorOptions[IN1, IN2, OUT]) *OperatorStream[IN1, IN2, OUT] {
	if leftUpstream.Env() != rightUpstream.Env() {
		panic("cannot add streams from multiple environments")
	}
	env := leftUpstream.Env()
	operatorOptions.Options.QOS = env.qos
	operatorOptions.Options.BarrierSignalChan = env.barrierSignalChan
	operatorOptions.Options.BarrierTriggerChan = env.barrierTriggerChan
	operatorOptions.Options.StoreManager = store.NewNonPersistenceManager(operatorOptions.Name(), env.storeBackend)
	outputStream := &OperatorStream[IN1, IN2, OUT]{
		env:                 env,
		once:                &sync.Once{},
		OperatorOptions:     operatorOptions,
		upstreamMap:         map[string]struct{}{},
		downstreamInitFnMap: map[string]downstreamInitFn[OUT]{},
	}
	leftUpstream.addDownstream(outputStream.Name(), outputStream.Init1)
	rightUpstream.addDownstream(outputStream.Name(), outputStream.Init2)
	outputStream.addUpstream(leftUpstream.Name())
	outputStream.addUpstream(rightUpstream.Name())
	return outputStream
}
