package stream

import (
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
)

type OperatorStreamOptions struct {
	Name     string
	Operator operator.NormalOperator
}

type OperatorStream struct {
	options             OperatorStreamOptions
	env                 *Environment
	downstreamInitFnMap map[string]downstreamInitFn
	upstreamMap         map[string]struct{}

	once      *sync.Once
	task      *task.Task
	chainTask []*task.Task
	initErr   error
}

func (o *OperatorStream) Name() string {
	return o.options.Name
}

func (o *OperatorStream) Env() *Environment {
	return o.env
}

func (o *OperatorStream) addUpstream(name string) {
	o.upstreamMap[name] = struct{}{}
}

func (o *OperatorStream) addDownstream(name string, downstreamInitFn downstreamInitFn) {
	o.downstreamInitFnMap[name] = downstreamInitFn
}

func (o *OperatorStream) Init(index int) func() (task.Emit, []*task.Task, error) {
	return func() (task.Emit, []*task.Task, error) {
		o.init()
		return o.task.InitEmit(index), o.chainTask, o.initErr
	}
}

func (o *OperatorStream) init() {
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
		manager, err := store.NewManager(o.options.Name, o.env.storeBackend)
		if err != nil {
			o.initErr = err
			return
		}
		taskOptions := task.Options{
			Name:              o.options.Name,
			BarrierSignalChan: o.env.barrierSignalChan,
			BufferSize:        o.env.options.BufferSize,

			DataEmit: func(e task.Data) {
				for _, emit := range emits {
					emit(e)
				}
			},
			Operator:     o.options.Operator,
			StoreManager: manager,
		}

		o.task = task.New(taskOptions)
		o.chainTask = append(allDownstreamTasks, o.task)

	})
}

func ApplyOneInput[IN, OUT any](upstream Stream[IN], streamOptions OperatorStreamOptions) (Stream[OUT], error) {

	//add operator prefix
	streamOptions.Name = "operator." + streamOptions.Name
	outputStream := &OperatorStream{
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

func ApplyTwoInput[IN1, IN2 any](leftUpstream Stream[IN1], rightUpstream Stream[IN2], streamOptions OperatorStreamOptions) (*OperatorStream, error) {
	if leftUpstream.Env() != rightUpstream.Env() {
		return nil, ErrMultipleEnv
	}
	//add operator prefix
	streamOptions.Name = "operator." + streamOptions.Name
	outputStream := &OperatorStream{
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
