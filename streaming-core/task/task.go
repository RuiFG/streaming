package task

import (
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"sync"
)

type Task struct {
	name           string
	status         *status.Status
	done           chan struct{}
	logger         *zap.Logger
	storeManager   store.Manager
	scope          tally.Scope
	executorChan   chan *executor.Executor
	normalOperator operator.NormalOperator

	rwMutex *sync.RWMutex

	inputChan  chan internalData
	inputCount int

	barrierAligner    *BarrierAligner
	barrierSignalChan chan<- Signal

	elementEmit element.Emit
	dataEmit    Emit

	//metrics
	inputTotalCounter          tally.Counter
	outputTotalCounter         tally.Counter
	lastCheckpointGauge        tally.Gauge
	failedCheckpointCounter    tally.Counter
	succeededCheckpointCounter tally.Counter
	executeHistogram           tally.Histogram
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

		if err := t.normalOperator.Open(operator.NewContext(
			t.logger,
			t.scope,
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
				stopwatch := t.executeHistogram.Start()
				exec.Exec()
				stopwatch.Stop()
			case data := <-t.inputChan:
				t.inputTotalCounter.Inc(1)
				t.barrierAligner.Handler(data)
				bufferSize := len(t.inputChan)
				for i := 0; i < bufferSize; i++ {
					t.inputTotalCounter.Inc(1)
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
			t.logger.Error("failed to close.", zap.Error(err))
		}
		t.logger.Info("stopped")
	}

}

func (t *Task) Emit(element element.NormalElement) {
	t.outputTotalCounter.Inc(1)
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
	t.lastCheckpointGauge.Update(float64(barrier.CheckpointId))
	//FIXME  the barrier ignored an event.
	t.rwMutex.Lock()
	t.elementEmit = t.MutexEmit
	t.NotifyBarrierCome(barrier)
	t.Emit(barrier)
	t.rwMutex.Unlock()
	t.elementEmit = t.Emit
	message := ACK
	if err := t.storeManager.Save(barrier.CheckpointId); err != nil {
		t.failedCheckpointCounter.Inc(1)
		t.logger.Warn("trigger barrier unable to ack.", zap.String("task", t.name), zap.Error(err))
		message = DEC
	} else {
		t.succeededCheckpointCounter.Inc(1)
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
		logger:            options.Logger,
		storeManager:      options.StoreManager,
		scope:             options.Scope,
		executorChan:      make(chan *executor.Executor, options.BufferSize),
		normalOperator:    options.Operator,
		rwMutex:           &sync.RWMutex{},
		inputChan:         make(chan internalData, options.BufferSize),
		inputCount:        0,
		barrierSignalChan: options.BarrierSignalChan,
		dataEmit:          options.DataEmit,

		inputTotalCounter:          options.Scope.Counter("input_total"),
		outputTotalCounter:         options.Scope.Counter("output_total"),
		lastCheckpointGauge:        options.Scope.Gauge("last_checkpoint"),
		failedCheckpointCounter:    options.Scope.Counter("failed_checkpoint"),
		succeededCheckpointCounter: options.Scope.Counter("succeeded_checkpoint"),
		executeHistogram:           options.Scope.Histogram("execute_duration", nil),
	}

}
