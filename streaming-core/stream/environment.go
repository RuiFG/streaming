package stream

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
	"time"
)

type EnvironmentOptions struct {
	//periodic checkpoints, if set 0, will not be enabled
	EnablePeriodicCheckpoint time.Duration
	//checkpoint state storage directory
	CheckpointsDir string
	//number of checkpoint state saved in storage
	CheckpointsNumRetained int
	//the interval between two checkpoint
	MinPauseBetweenCheckpoints time.Duration
	//maximum number of checkpoint failures allowed
	TolerableCheckpointFailureNumber int
	//maximum number of concurrent checkpoints
	MaxConcurrentCheckpoints int
	//one checkpoint timeout
	CheckpointTimeout time.Duration
	//maximum number of event buffer by each operator
	BufferSize int
}

var DefaultEnvironmentOptions = EnvironmentOptions{
	EnablePeriodicCheckpoint:         0,
	CheckpointsDir:                   ".",
	CheckpointsNumRetained:           2,
	MinPauseBetweenCheckpoints:       60 * time.Second,
	TolerableCheckpointFailureNumber: 5,
	MaxConcurrentCheckpoints:         2,
	CheckpointTimeout:                10 * time.Second,
	BufferSize:                       2048,
}

// Environment is stream environment, every stream application needs the support of the *Environment.
type Environment struct {
	ctx               _c.Context
	cancel            _c.CancelFunc
	logger            log.Logger
	options           EnvironmentOptions
	barrierSignalChan chan task.Signal
	sourceInitFns     []sourceInitFn
	coordinator       *task.Coordinator
	storeBackend      store.Backend
	allChainTasks     []*task.Task

	errorChan chan error
}

func (e *Environment) addSourceInit(fn sourceInitFn) {
	e.sourceInitFns = append(e.sourceInitFns, fn)
}

func (e *Environment) Start() error {
	var rootTasks []*task.Task
	if len(e.sourceInitFns) <= 0 {
		return errors.Errorf("source init fn should not be empty")
	}
	//0. check stream graph and print

	//1. init all task
	for _, initFn := range e.sourceInitFns {
		if rootTask, chainTasks, err := initFn(); err != nil {
			return errors.WithMessage(err, "failed to init task")
		} else {
			if rootTask != nil {
				rootTasks = append(rootTasks, rootTask)
			}
			if chainTasks != nil {
				e.allChainTasks = append(e.allChainTasks, chainTasks...)
			}
		}
	}

	//2. verify that dag is compliant

	//3. start all chain task
	for _, _task := range e.allChainTasks {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	e.startMonitor()
	//4. waiting task running
	for _, _task := range e.allChainTasks {
		for !_task.Running() {
			select {
			case <-e.ctx.Done():
				e.logger.Error("the current application has been stopped, interrupting task")
				return errors.Errorf("unable to start the application")
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	//5. start coordinator
	e.coordinator = task.NewCoordinator(rootTasks, e.allChainTasks, e.storeBackend, e.barrierSignalChan,
		e.options.MaxConcurrentCheckpoints, e.options.MinPauseBetweenCheckpoints, e.options.CheckpointTimeout, e.options.TolerableCheckpointFailureNumber)
	e.coordinator.Activate()

	if e.options.EnablePeriodicCheckpoint > 0 {
		e.startPeriodicCheckpoint()
	}

	return nil
}

func (e *Environment) Stop() error {
	if e.coordinator != nil {
		e.coordinator.Deactivate()
		e.coordinator.Wait()
	}
	for _, _task := range e.allChainTasks {
		_task.Close()
	}
	return e.storeBackend.Close()
}

func (e *Environment) startMonitor() {
	go func() {
		err := <-e.errorChan
		if err != nil {
			e.logger.Error("monitored task error.", err)
			e.cancel()
		}
	}()
}

func (e *Environment) startPeriodicCheckpoint() {
	go func() {
		ticker := time.NewTicker(e.options.EnablePeriodicCheckpoint)
		for true {
			select {
			case <-e.ctx.Done():
				return
			case <-ticker.C:
				e.coordinator.TriggerCheckpoint()
			}
		}
	}()
}

func New(options EnvironmentOptions) (*Environment, error) {
	var storeBackend store.Backend

	if fs, err := store.NewFSBackend(options.CheckpointsDir, options.CheckpointsNumRetained); err != nil {
		return nil, errors.WithMessage(err, "failed to new fs store backend")
	} else {
		storeBackend = fs
	}
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Environment{
		ctx:               ctx,
		cancel:            cancelFunc,
		logger:            log.Global().Named("environment"),
		options:           options,
		barrierSignalChan: make(chan task.Signal),
		storeBackend:      storeBackend,
		errorChan:         make(chan error),
	}, nil
}
