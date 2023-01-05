package task

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Task struct {
	name         string
	status       *status.Status
	done         chan struct{}
	logger       *zap.Logger
	storeManager store.Manager
	scope        tally.Scope
	//locker is task global locker, sourceEmit, processElement or execExecutor need hold it
	locker       sync.Locker
	executorChan chan *executor.Executor

	operator operator.Operator

	inputChan  chan internalData
	inputCount int

	barrierAligner    *BarrierAligner
	barrierSignalChan chan<- Signal

	dataEmit Emit

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
		t.barrierAligner = NewBarrierAligner(t, t, t.inputCount)

		if err := t.operator.Open(operator.NewContext(
			t.logger,
			t.scope,
			t.storeManager.Controller(t.Name()),
			t.executorChan, operator.NewTimerManager(), t.locker),
			t.Emit); err != nil {
			return fmt.Errorf("failed to start task: %w", err)
		}
		t.logger.Info("started")
		//TODO: use priority mailbox
		for {
			select {
			case <-t.done:
				return nil
			case exec := <-t.executorChan:
				t.locker.Lock()
				stopwatch := t.executeHistogram.Start()
				exec.Exec()
				stopwatch.Stop()
				t.locker.Unlock()
			case data := <-t.inputChan:
				t.locker.Lock()
				t.inputTotalCounter.Inc(1)
				t.barrierAligner.Handler(data)
				bufferSize := len(t.inputChan)
				for i := 0; i < bufferSize; i++ {
					t.inputTotalCounter.Inc(1)
					t.barrierAligner.Handler(<-t.inputChan)
				}
				t.locker.Unlock()
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
		if err := t.operator.Close(); err != nil {
			t.logger.Error("failed to close.", zap.Error(err))
		}
		t.logger.Info("stopped")
	}

}

func (t *Task) Emit(element element.Element) {
	t.outputTotalCounter.Inc(1)
	t.dataEmit(element)
}

// -------------------------------------Processor---------------------------------------------

func (t *Task) ProcessData(data internalData) {
	t.operator.ProcessElement(data.eob, data.index)
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (t *Task) TriggerBarrier(barrier Barrier) {
	t.lastCheckpointGauge.Update(float64(barrier.CheckpointId))
	t.NotifyBarrierCome(barrier)
	t.Emit(barrier)
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
	t.operator.NotifyCheckpointCome(barrier.CheckpointId)

}

func (t *Task) NotifyBarrierComplete(barrier Barrier) {
	t.executorChan <- executor.NewExecutor(func() {
		t.operator.NotifyCheckpointComplete(barrier.CheckpointId)
	})
}

func (t *Task) NotifyBarrierCancel(barrier Barrier) {
	t.executorChan <- executor.NewExecutor(func() {
		t.operator.NotifyCheckpointCancel(barrier.CheckpointId)
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
		operator:          options.Operator,
		locker:            &sync.Mutex{},
		inputChan:         make(chan internalData, options.BufferSize),
		inputCount:        0,
		barrierSignalChan: options.BarrierSignalChan,
		dataEmit:          options.DataEmit,

		inputTotalCounter:          options.Scope.Counter("input_total"),
		outputTotalCounter:         options.Scope.Counter("output_total"),
		lastCheckpointGauge:        options.Scope.Gauge("last_checkpoint"),
		failedCheckpointCounter:    options.Scope.Counter("failed_checkpoint"),
		succeededCheckpointCounter: options.Scope.Counter("succeeded_checkpoint"),
		executeHistogram:           options.Scope.Histogram("execute_duration", tally.DurationBuckets{5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second}),
	}

}
