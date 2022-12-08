package task

import (
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
	status         status.Status
	done           chan struct{}
	logger         log.Logger
	storeManager   store.Manager
	callerChan     chan func()
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
		t.inputChan <- internalData{
			index: index,
			eob:   eob,
		}
	}
}

func (t *Task) Daemon() error {
	if status.CAP(&t.status, status.Ready, status.Running) {
		t.elementEmit = t.Emit
		t.barrierAligner = NewBarrierAligner(t, t, t.inputCount)

		if err := t.normalOperator.Open(operator.NewContext(t.logger.Named("operator"),
			t.storeManager.Controller(t.Name()),
			t.callerChan, operator.NewTimerManager()),
			t.elementEmit); err != nil {
			return errors.WithMessage(err, "failed to start task")
		}
		t.logger.Info("started")
		//TODO: use priority mailbox and discard rwMutex
		for {
			select {
			case <-t.done:
				return nil
			case caller := <-t.callerChan:
				caller()
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
	if status.CAP(&t.status, status.Running, status.Closed) {
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

// -------------------------------------BarrierListener-------------------------------------

func (t *Task) NotifyBarrierCome(barrier Barrier) {
	t.normalOperator.NotifyCheckpointCome(barrier.CheckpointId)

}

func (t *Task) NotifyBarrierComplete(barrier Barrier) {
	t.callerChan <- func() {
		t.normalOperator.NotifyCheckpointComplete(barrier.CheckpointId)
	}
}

func (t *Task) NotifyBarrierCancel(barrier Barrier) {
	t.callerChan <- func() {
		t.normalOperator.NotifyCheckpointCancel(barrier.CheckpointId)
	}
}

func New(options Options) *Task {
	return &Task{
		name:              options.Name,
		status:            status.Ready,
		done:              make(chan struct{}),
		logger:            log.Global().Named(options.Name + ".task"),
		rwMutex:           &sync.RWMutex{},
		inputChan:         make(chan internalData, options.BufferSize),
		inputCount:        0,
		normalOperator:    options.Operator,
		callerChan:        make(chan func(), options.BufferSize),
		dataEmit:          options.DataEmit,
		storeManager:      options.StoreManager,
		barrierSignalChan: options.BarrierSignalChan,
	}

}
