package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/pkg/errors"
	"sync"
)

type Task struct {
	ctx        _c.Context
	cancelFunc _c.CancelFunc
	logger     log.Logger
	Options

	running bool
	rwMutex *sync.RWMutex

	inputChan chan internalData

	normalOperator operator.NormalOperator
	callerChan     chan func()

	barrierAligner *BarrierAligner

	elementEmit element.Emit
}

func (o *Task) Name() string {
	return o.Options.Name
}

func (o *Task) Daemon() error {
	o.logger.Info("staring...")

	o.inputChan = make(chan internalData, o.ChannelSize)
	o.elementEmit = o.Emit
	o.barrierAligner = NewBarrierAligner(o, o, o.Options.InputCount)

	if err := safe.Run(func() error {
		return o.normalOperator.Open(operator.NewContext(o.logger.Named("operator"),
			o.StoreManager.Controller(o.Name()),
			o.callerChan, operator.NewTimerManager()),
			o.elementEmit)
	}); err != nil {
		return errors.WithMessage(err, "failed to start task")
	}
	o.running = true
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

func (o *Task) Emit(element element.NormalElement) {
	o.EmitNext(element)
}

func (o *Task) MutexEmit(e element.NormalElement) {
	o.rwMutex.RLock()
	o.Emit(e)
	o.rwMutex.RUnlock()
}

func (o *Task) InitEmit(index int) Emit {
	return func(eob Data) {
		o.inputChan <- internalData{
			index: index,
			eob:   eob,
		}
	}
}

// -------------------------------------Processor---------------------------------------------

func (o *Task) ProcessData(data internalData) {
	o.normalOperator.ProcessElement(data.eob, data.index)
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (o *Task) TriggerBarrier(barrier Barrier) {
	o.rwMutex.Lock()
	o.elementEmit = o.MutexEmit
	o.Emit(barrier)
	o.NotifyBarrierCome(barrier)
	o.rwMutex.Unlock()
	o.elementEmit = o.Emit
	message := ACK
	if err := o.StoreManager.Save(barrier.Id); err != nil {
		message = DEC
	}
	o.BarrierSignalChan <- Signal{
		Name:    o.Name(),
		Message: message,
		Barrier: barrier,
	}

}

// -------------------------------------BarrierListener-------------------------------------

func (o *Task) NotifyBarrierCome(barrier Barrier) {
	o.normalOperator.NotifyCheckpointCome(barrier.Id)
}

func (o *Task) NotifyBarrierComplete(barrier Barrier) {
	o.normalOperator.NotifyCheckpointComplete(barrier.Id)
	switch barrier.BarrierType {
	case ExitpointBarrier:
		o.cancelFunc()
	}
}

func (o *Task) NotifyBarrierCancel(barrier Barrier) {
	o.normalOperator.NotifyCheckpointCancel(barrier.Id)

}

func New(options Options) *Task {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Task{
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		logger:         log.Global().Named(options.Name + ".task"),
		Options:        options,
		running:        false,
		rwMutex:        &sync.RWMutex{},
		normalOperator: options.Operator,
		callerChan:     make(chan func()),
	}
}
