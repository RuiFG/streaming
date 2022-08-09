package stream

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/safe"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
	"time"
)

type Options struct {
	QOS      task.QOS
	State    bool
	StateDir string
}

type Env struct {
	ctx                _c.Context
	cancel             _c.CancelFunc
	sourceInitFns      []sourceInitFn
	errorChan          chan error
	coordinator        *task.Coordinator
	barrierSignalChan  chan element.Signal
	barrierTriggerChan chan element.BarrierType
	storeBackend       store.Backend
	qos                task.QOS
}

func (e *Env) addSourceInit(fn sourceInitFn) {
	e.sourceInitFns = append(e.sourceInitFns, fn)
}

func (e *Env) Start() error {
	var (
		rootTasks    []task.Task
		nonRootTasks []task.Task
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
	e.coordinator = task.NewCoordinator(5*time.Second, rootTasks, append(rootTasks, nonRootTasks...), e.storeBackend, e.barrierSignalChan, e.barrierTriggerChan)
	//2. verify that dag is compliant

	//3.1 start all task
	for _, _task := range nonRootTasks {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	//3.2 waiting task running
	for _, _task := range nonRootTasks {
		for !_task.Running() {
			time.Sleep(time.Millisecond)
		}
	}

	for _, _task := range rootTasks {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	//4. start coordinator
	e.coordinator.Activate()
	return nil
}

func (e *Env) Stop() error {
	e.coordinator.Deactivate()
	e.coordinator.Wait()
	return nil
}

func New(options Options) *Env {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Env{
		ctx:                ctx,
		cancel:             cancelFunc,
		errorChan:          make(chan error),
		barrierSignalChan:  make(chan element.Signal),
		barrierTriggerChan: make(chan element.BarrierType),
		qos:                options.QOS,
		storeBackend:       store.NewMemoryBackend(),
	}
}

func NewWithCtx(ctx _c.Context, options Options) {
	env := New(options)
	safe.Go(func() error {
		<-ctx.Done()
		return env.Stop()
	})
}
