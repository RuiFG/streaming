package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"sync"
)

type Task struct {
	name           string
	ctx            _c.Context
	cancelFunc     _c.CancelFunc
	logger         log.Logger
	storeManager   store.Manager
	callerChan     chan func()
	normalOperator operator.NormalOperator
	running        bool
	rwMutex        *sync.RWMutex

	inputChan  chan internalData
	inputCount int

	barrierAligner    *BarrierAligner
	barrierSignalChan chan<- Signal

	elementEmit element.Emit
	dataEmit    Emit
}

func (o *Task) Name() string {
	return o.name
}

// InitEmit will be called multiple times before Daemon run
func (o *Task) InitEmit(index int) Emit {
	if o.running {
		panic("the task is already running. It is forbidden to call")
	}
	o.inputCount += 1
	return func(eob Data) {
		o.inputChan <- internalData{
			index: index,
			eob:   eob,
		}
	}
}

func (o *Task) Daemon() error {
	o.logger.Info("staring...")
	o.elementEmit = o.Emit
	o.barrierAligner = NewBarrierAligner(o, o, o.inputCount)

	if err := o.normalOperator.Open(operator.NewContext(o.logger.Named("operator"),
		o.storeManager.Controller(o.Name()),
		o.callerChan, operator.NewTimerManager()),
		o.elementEmit); err != nil {
		return errors.WithMessage(err, "failed to start task")
	}
	o.running = true
	//TODO: use priority mailbox and discard rwMutex
	for {
		select {
		case <-o.ctx.Done():
			o.running = false
			if err := o.normalOperator.Close(); err != nil {
				return err
			}
			return nil
		case caller := <-o.callerChan:
			caller()
		case e1 := <-o.inputChan:
			o.barrierAligner.Handler(e1)
			bufferSize := len(o.inputChan)
			for i := 0; i < bufferSize; i++ {
				o.barrierAligner.Handler(<-o.inputChan)
			}
		}
	}
}

func (o *Task) Running() bool {
	return o.running
}

func (o *Task) Close() {
	o.cancelFunc()
}

func (o *Task) Emit(element element.NormalElement) {
	o.dataEmit(element)
}

func (o *Task) MutexEmit(e element.NormalElement) {
	o.rwMutex.RLock()
	o.Emit(e)
	o.rwMutex.RUnlock()
}

// -------------------------------------Processor---------------------------------------------

func (o *Task) ProcessData(data internalData) {
	o.normalOperator.ProcessElement(data.eob, data.index)
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (o *Task) TriggerBarrier(barrier Barrier) {
	o.rwMutex.Lock()
	o.elementEmit = o.MutexEmit
	o.NotifyBarrierCome(barrier)
	o.Emit(barrier)
	o.rwMutex.Unlock()
	o.elementEmit = o.Emit
	message := ACK
	if err := o.storeManager.Save(barrier.CheckpointId); err != nil {
		o.logger.Warnw("trigger barrier unable to complete", "err", err, "task", o.name)
		message = DEC
	}
	o.barrierSignalChan <- Signal{
		Name:    o.Name(),
		Message: message,
		Barrier: barrier,
	}
}

// -------------------------------------BarrierListener-------------------------------------

func (o *Task) NotifyBarrierCome(barrier Barrier) {
	o.normalOperator.NotifyCheckpointCome(barrier.CheckpointId)

}

func (o *Task) NotifyBarrierComplete(barrier Barrier) {
	o.callerChan <- func() {
		o.normalOperator.NotifyCheckpointComplete(barrier.CheckpointId)
	}
}

func (o *Task) NotifyBarrierCancel(barrier Barrier) {
	o.callerChan <- func() {
		o.normalOperator.NotifyCheckpointCancel(barrier.CheckpointId)
	}
}

func New(options Options) *Task {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Task{
		name:              options.Name,
		ctx:               ctx,
		cancelFunc:        cancelFunc,
		logger:            log.Global().Named(options.Name + ".task"),
		running:           false,
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
