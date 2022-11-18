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
	Operator         operator.NormalOperator
	ElementListeners []element.Listener[IN1, IN2, OUT]
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
	downstreamInitFnMap map[string]downstreamInitFn
	upstreamMap         map[string]struct{}

	once           *sync.Once
	task           *task.Task
	downstreamTask []*task.Task
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

func (o *OperatorStream[IN1, IN2, OUT]) addDownstream(name string, downstreamInitFn downstreamInitFn) {
	o.downstreamInitFnMap[name] = downstreamInitFn
}

func (o *OperatorStream[IN1, IN2, OUT]) Init(index int) func() (task.Emit, []*task.Task, error) {
	return func() (task.Emit, []*task.Task, error) {
		o.init()
		return o.task.InitEmit(index), o.downstreamTask, o.initErr
	}
}

func (o *OperatorStream[IN1, IN2, OUT]) init() {
	o.once.Do(func() {
		var (
			emits              []task.Emit
			allDownstreamTasks []*task.Task
		)
		for _, initFn := range o.downstreamInitFnMap {
			if emitNext, downstreamTasks, err := initFn(); err != nil {
				o.initErr = err
				return
			} else {
				emits = append(emits, emitNext)
				if downstreamTasks != nil {
					allDownstreamTasks = append(allDownstreamTasks, downstreamTasks...)
				}
			}
		}
		taskOptions := task.Options{
			Name:               o.options.Name,
			QOS:                o.env.qos,
			BarrierSignalChan:  o.env.barrierSignalChan,
			BarrierTriggerChan: o.env.barrierTriggerChan,
			ChannelSize:        o.options.ChannelSize,

			EmitNext: func(e task.Data) {
				for _, emit := range emits {
					emit(e)
				}
			},
			Operator:     o.options.Operator,
			InputCount:   len(o.upstreamMap),
			OutputCount:  len(o.downstreamInitFnMap),
			StoreManager: store.NewManager(o.options.Name, o.env.storeBackend),
		}

		o.task = task.New(taskOptions)
		o.downstreamTask = append(allDownstreamTasks, o.task)

	})
}

func ApplyOneInput[IN, OUT any](upstream Stream[IN], streamOptions OperatorStreamOptions[IN, any, OUT]) (*OperatorStream[IN, any, OUT], error) {

	//add operator prefix
	streamOptions.Name = "operator." + streamOptions.Name
	outputStream := &OperatorStream[IN, any, OUT]{
		options:             streamOptions,
		env:                 upstream.Env(),
		once:                &sync.Once{},
		upstreamMap:         map[string]struct{}{},
		downstreamInitFnMap: map[string]downstreamInitFn{},
	}
	upstream.addDownstream(outputStream.Name(), outputStream.Init(0))
	outputStream.addUpstream(upstream.Name())
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
		downstreamInitFnMap: map[string]downstreamInitFn{},
	}
	leftUpstream.addDownstream(outputStream.Name(), outputStream.Init(0))
	rightUpstream.addDownstream(outputStream.Name(), outputStream.Init(1))
	outputStream.addUpstream(leftUpstream.Name())
	outputStream.addUpstream(rightUpstream.Name())
	return outputStream, nil
}
