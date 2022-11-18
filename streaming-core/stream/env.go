package stream

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
	"time"
)

type EnvOptions struct {
	QOS task.QOS
	//state save dir
	StateDir string
}

// Env is stream environment, every stream application needs the support of the *Env.
type Env struct {
	ctx                _c.Context
	cancel             _c.CancelFunc
	sourceInitFns      []sourceInitFn
	errorChan          chan error
	coordinator        *task.Coordinator
	barrierSignalChan  chan task.Signal
	barrierTriggerChan chan task.BarrierType
	storeBackend       store.Backend
	qos                task.QOS
}

func (e *Env) addSourceInit(fn sourceInitFn) {
	e.sourceInitFns = append(e.sourceInitFns, fn)
}

func (e *Env) Start() error {
	var (
		rootTasks    []*task.Task
		nonRootTasks []*task.Task
	)
	//init all
	for _, initFn := range e.sourceInitFns {
		if rootTask, subNonRootTasks, err := initFn(); err != nil {
			return errors.WithMessage(err, "failed to init")
		} else {
			if rootTask != nil {
				rootTasks = append(rootTasks, rootTask)
			}
			if subNonRootTasks != nil {
				nonRootTasks = append(nonRootTasks, subNonRootTasks...)
			}
		}
	}

	//2. verify that dag is compliant

	//3.1 start non root task
	for _, _task := range nonRootTasks {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	//3.2 waiting task running
	for _, _task := range nonRootTasks {
		for !_task.Running() {
			time.Sleep(time.Millisecond)
		}
	}
	//3.3 start root task
	for _, _task := range rootTasks {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	//3.4 waiting task running
	for _, _task := range rootTasks {
		for !_task.Running() {
			time.Sleep(time.Millisecond)
		}
	}
	//4. start coordinator
	e.coordinator = task.NewCoordinator(rootTasks, append(rootTasks, nonRootTasks...), e.storeBackend, e.barrierSignalChan, e.barrierTriggerChan)
	e.coordinator.Activate()
	return nil
}

func (e *Env) Stop() error {
	e.coordinator.Deactivate()
	e.coordinator.Wait()
	return nil
}

func (e *Env) RegisterBarrierTrigger(register func(chan<- task.BarrierType)) {
	safe.Go(func() error {
		register(e.barrierTriggerChan)
		return nil
	})
}

func New(options EnvOptions) (*Env, error) {
	var storeBackend store.Backend
	switch options.QOS {
	case task.AtMostOnce:
		storeBackend = store.NewMemoryBackend()
	case task.AtLeastOnce, task.ExactlyOnce:
		if fs, err := store.NewFs(options.StateDir, 5); err != nil {
			return nil, errors.WithMessage(err, "failed to new fs store backend")
		} else {
			storeBackend = fs
		}
	}

	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Env{
		ctx:                ctx,
		cancel:             cancelFunc,
		errorChan:          make(chan error),
		barrierSignalChan:  make(chan task.Signal),
		barrierTriggerChan: make(chan task.BarrierType, 1),
		qos:                options.QOS,
		storeBackend:       storeBackend,
	}, nil
}

func NewWithCtx(ctx _c.Context, options EnvOptions) (*Env, error) {
	env, err := New(options)
	if err != nil {
		return nil, err
	} else {
		safe.Go(func() error {
			<-ctx.Done()
			return env.Stop()
		})
		return env, nil
	}
}
