package task

import (
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"sync"
)

type Task struct {
	name           string
	status         *status.Status
	done           chan struct{}
	logger         log.Logger
	storeManager   store.Manager
	executorChan   chan *executor.Executor
	normalOperator operator.NormalOperator

	rwMutex *sync.RWMutex

	inputChan  chan internalData
	inputCount int

	barrierAligner    *BarrierAligner
	barrierSignalChan chan<- Signal

	elementEmit element.Emit
	dataEmit    Emit
}

func (t *Task) Name() string {
	return t.name
}

// InitEmit will be called multiple times before Daemon run
func (t *Task) InitEmit(index int) Emit {
	t.inputCount += 1
	return func(eob Data) {
		select {
		case t.inputChan <- internalData{
			index: index,
			eob:   eob,
		}:
		case <-t.done:
		}
	}
}

func (t *Task) Daemon() error {
	if t.status.CAS(status.Ready, status.Running) {
		t.elementEmit = t.Emit
		t.barrierAligner = NewBarrierAligner(t, t, t.inputCount)

		if err := t.normalOperator.Open(operator.NewContext(t.logger.Named("operator"),
			t.storeManager.Controller(t.Name()),
			t.executorChan, operator.NewTimerManager()),
			t.elementEmit); err != nil {
			return errors.WithMessage(err, "failed to start task")
		}
		t.logger.Info("started")
		//TODO: use priority mailbox and discard rwMutex
		for {
			select {
			case <-t.done:
				return nil
			case exec := <-t.executorChan:
				exec.Exec()
			case e1 := <-t.inputChan:
				t.barrierAligner.Handler(e1)
				bufferSize := len(t.inputChan)
				for i := 0; i < bufferSize; i++ {
					t.barrierAligner.Handler(<-t.inputChan)
				}
			}
		}
	}
	return nil
}

func (t *Task) Running() bool {
	return t.status.Running()
}

func (t *Task) Close() {
	if t.status.CAS(status.Running, status.Closed) {
		close(t.done)
		if err := t.normalOperator.Close(); err != nil {
			t.logger.Errorf("failed to status", "err", err)
		}
		t.logger.Info("stopped")
	}

}

func (t *Task) Emit(element element.NormalElement) {
	t.dataEmit(element)
}

func (t *Task) MutexEmit(e element.NormalElement) {
	t.rwMutex.RLock()
	t.Emit(e)
	t.rwMutex.RUnlock()
}

// -------------------------------------Processor---------------------------------------------

func (t *Task) ProcessData(data internalData) {
	t.normalOperator.ProcessElement(data.eob, data.index)
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (t *Task) TriggerBarrier(barrier Barrier) {
	//FIXME  the barrier ignored an event.
	t.rwMutex.Lock()
	t.elementEmit = t.MutexEmit
	t.NotifyBarrierCome(barrier)
	t.Emit(barrier)
	t.rwMutex.Unlock()
	t.elementEmit = t.Emit
	message := ACK
	if err := t.storeManager.Save(barrier.CheckpointId); err != nil {
		t.logger.Warnw("trigger barrier unable to ack", "err", err, "task", t.name)
		message = DEC
	}
	t.barrierSignalChan <- Signal{
		Name:    t.Name(),
		Message: message,
		Barrier: barrier,
	}
}

func (t *Task) TriggerBarrierAsync(barrier Barrier) {
	t.executorChan <- executor.NewExecutor(func() {
		t.TriggerBarrier(barrier)
	})
}

// -------------------------------------BarrierListener-------------------------------------

func (t *Task) NotifyBarrierCome(barrier Barrier) {
	t.normalOperator.NotifyCheckpointCome(barrier.CheckpointId)

}

func (t *Task) NotifyBarrierComplete(barrier Barrier) {
	t.executorChan <- executor.NewExecutor(func() {
		t.normalOperator.NotifyCheckpointComplete(barrier.CheckpointId)
	})
}

func (t *Task) NotifyBarrierCancel(barrier Barrier) {
	t.executorChan <- executor.NewExecutor(func() {
		t.normalOperator.NotifyCheckpointCancel(barrier.CheckpointId)
	})

}

func New(options Options) *Task {
	return &Task{
		name:              options.Name,
		status:            status.NewStatus(status.Ready),
		done:              make(chan struct{}),
		logger:            log.Global().Named(options.Name + ".task"),
		rwMutex:           &sync.RWMutex{},
		inputChan:         make(chan internalData, options.BufferSize),
		inputCount:        0,
		normalOperator:    options.Operator,
		executorChan:      make(chan *executor.Executor, options.BufferSize),
		dataEmit:          options.DataEmit,
		storeManager:      options.StoreManager,
		barrierSignalChan: options.BarrierSignalChan,
	}

}
