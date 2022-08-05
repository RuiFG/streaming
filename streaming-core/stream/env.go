package stream

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/barrier"
	"github.com/RuiFG/streaming/streaming-core/safe"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
	"time"
)

type Env struct {
	name          string
	ctx           _c.Context
	cancel        _c.CancelFunc
	sourceInitFns []sourceInitFn
	errorChan     chan error
	coordinator   *barrier.Coordinator

	barrierSignalChan  chan barrier.Signal
	barrierTriggerChan chan barrier.Type
}

func (e *Env) AddSourceInit(fn sourceInitFn) {
	e.sourceInitFns = append(e.sourceInitFns, fn)
}

func (e *Env) Start() error {
	var (
		tasksToTrigger []task.Task
		tasksToWaitFor []task.Task
	)
	//init all
	for _, initFn := range e.sourceInitFns {
		if _tasksToTrigger, _tasksToWaitFor, err := initFn(); err != nil {
			return errors.WithMessage(err, "failed to init")
		} else {
			if _tasksToTrigger != nil {
				tasksToTrigger = append(tasksToTrigger, _tasksToTrigger...)
			}
			if _tasksToWaitFor != nil {
				tasksToWaitFor = append(tasksToWaitFor, _tasksToWaitFor...)
			}
		}
	}
	e.coordinator = barrier.NewCoordinator(1*time.Minute, tasksToTrigger, tasksToWaitFor, nil, e.barrierSignalChan, e.barrierTriggerChan)
	//2. verify that dag is compliant

	//3. start all task
	for _, _task := range tasksToWaitFor {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	for _, _task := range tasksToTrigger {
		safe.GoChannel(_task.Daemon, e.errorChan)
	}
	//4. start coordinator
	e.coordinator.Activate()
	return nil
}

func (e *Env) Stop() error {
	e.coordinator.Deactivate()
	e.coordinator.Wait()
	return
}

func New() *Env {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Env{ctx: ctx, cancel: cancelFunc, errorChan: make(chan error), barrierSignalChan: make(chan barrier.Signal), barrierTriggerChan: make(chan barrier.Type)}
}