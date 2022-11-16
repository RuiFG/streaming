package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type OperatorStreamOptions[IN1, IN2, OUT any] struct {
	Options
	ElementListeners []element.Listener[IN1, IN2, OUT]
	New              operator.NewOperator[IN1, IN2, OUT]
}

type WithOperatorStreamOptions[IN1, IN2, OUT any] func(options OperatorStreamOptions[IN1, IN2, OUT]) OperatorStreamOptions[IN1, IN2, OUT]

func ApplyWithOperatorStreamOptionsFns[IN1, IN2, OUT any](applyFns []WithOperatorStreamOptions[IN1, IN2, OUT]) OperatorStreamOptions[IN1, IN2, OUT] {
	options := OperatorStreamOptions[IN1, IN2, OUT]{Options: Options{ChannelSize: 1024}}
	for _, fn := range applyFns {
		options = fn(options)
	}
	return options
}

type OperatorStream[IN1, IN2, OUT any] struct {
	options             OperatorStreamOptions[IN1, IN2, OUT]
	env                 *Env
	downstreamInitFnMap map[string]downstreamInitFn[OUT]
	upstreamMap         map[string]struct{}

	once           *sync.Once
	task           *task.OperatorTask[IN1, IN2, OUT]
	downstreamTask []task.Task
	initErr        error
}

func (o *OperatorStream[IN1, IN2, OUT]) Name() string {
	return o.options.Name
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

func (o *OperatorStream[IN1, IN2, OUT]) Init1(upstream string) (task.Emit, []task.Task, error) {
	o.init()
	return o.task.Emit1, o.downstreamTask, o.initErr
}

func (o *OperatorStream[IN1, IN2, OUT]) Init2(upstream string) (task.Emit, []task.Task, error) {
	o.init()
	return o.task.Emit2, o.downstreamTask, o.initErr
}

func (o *OperatorStream[IN1, IN2, OUT]) init() {
	o.once.Do(func() {
		var (
			emits              []task.Emit
			allDownstreamTasks []task.Task
		)
		for _, initFn := range o.downstreamInitFnMap {
			if emitNext, downstreamTasks, err := initFn(o.options.Name); err != nil {
				o.initErr = err
				return
			} else {
				emits = append(emits, emitNext)
				if downstreamTasks != nil {
					allDownstreamTasks = append(allDownstreamTasks, downstreamTasks...)
				}
			}
		}
		taskOptions := task.Options[IN1, IN2, OUT]{
			Name:               o.options.Name,
			QOS:                o.env.qos,
			BarrierSignalChan:  o.env.barrierSignalChan,
			BarrierTriggerChan: o.env.barrierTriggerChan,
			ChannelSize:        o.options.ChannelSize,
			ElementListeners:   o.options.ElementListeners,
			Emit: func(e task.ElementOrBarrier) {
				for _, emit := range emits {
					emit(e)
				}
			},
			New:          o.options.New,
			InputCount:   len(o.upstreamMap),
			OutputCount:  len(o.downstreamInitFnMap),
			StoreManager: store.NewManager(o.options.Name, o.env.storeBackend),
		}

		o.task = task.New[IN1, IN2, OUT](taskOptions)
		o.downstreamTask = append(allDownstreamTasks, o.task)

	})
}

func ApplyOneInput[IN, OUT any](upstreams []Stream[IN], streamOptions OperatorStreamOptions[IN, any, OUT]) (*OperatorStream[IN, any, OUT], error) {
	var env *Env
	for _, upstream := range upstreams {
		if env == nil {
			env = upstream.Env()
		} else if env != upstream.Env() {
			return nil, ErrMultipleEnv
		}
	}
	//add operator prefix
	streamOptions.Name = "operator." + streamOptions.Name
	outputStream := &OperatorStream[IN, any, OUT]{
		options:             streamOptions,
		env:                 env,
		once:                &sync.Once{},
		upstreamMap:         map[string]struct{}{},
		downstreamInitFnMap: map[string]downstreamInitFn[OUT]{},
	}
	for _, upstream := range upstreams {
		upstream.addDownstream(outputStream.Name(), outputStream.Init1)
		outputStream.addUpstream(upstream.Name())
	}
	return outputStream, nil
}

func ApplyTwoInput[IN1, IN2, OUT any](leftUpstream Stream[IN1], rightUpstream Stream[IN2], streamOptions OperatorStreamOptions[IN1, IN2, OUT]) (*OperatorStream[IN1, IN2, OUT], error) {
	if leftUpstream.Env() != rightUpstream.Env() {
		return nil, ErrMultipleEnv
	}
	//add operator prefix
	streamOptions.Name = "operator." + streamOptions.Name
	outputStream := &OperatorStream[IN1, IN2, OUT]{
		options:             streamOptions,
		env:                 leftUpstream.Env(),
		once:                &sync.Once{},
		upstreamMap:         map[string]struct{}{},
		downstreamInitFnMap: map[string]downstreamInitFn[OUT]{},
	}
	leftUpstream.addDownstream(outputStream.Name(), outputStream.Init1)
	rightUpstream.addDownstream(outputStream.Name(), outputStream.Init2)
	outputStream.addUpstream(leftUpstream.Name())
	outputStream.addUpstream(rightUpstream.Name())
	return outputStream, nil
}
