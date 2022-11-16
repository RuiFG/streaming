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

type OperatorTask[IN1, IN2, OUT any] struct {
	ctx        _c.Context
	cancelFunc _c.CancelFunc
	logger     log.Logger
	Options[IN1, IN2, OUT]

	running    bool
	rwMutex    *sync.RWMutex
	collector  *collector[OUT]
	input1Chan chan ElementOrBarrier
	input2Chan chan ElementOrBarrier

	operatorRuntime *operator.Runtime[IN1, IN2, OUT]
	callerChan      chan func()

	barrierAligner *BarrierAligner[IN1, IN2]
}

func (o *OperatorTask[IN1, IN2, OUT]) Name() string {
	return o.Options.Name
}

func (o *OperatorTask[IN1, IN2, OUT]) Daemon() error {
	o.logger.Info("staring...")
	o.operatorRuntime = &operator.Runtime[IN1, IN2, OUT]{
		Operator: o.New(),
	}
	o.collector = &collector[OUT]{upstream: o.Name(), Emit: o.OnElementOrBarrier}
	o.input1Chan = make(chan ElementOrBarrier, o.ChannelSize)
	o.input2Chan = make(chan ElementOrBarrier, o.ChannelSize)
	o.barrierAligner = NewBarrierAligner[IN1, IN2](o, o, o.Options.InputCount)

	if err := safe.Run(func() error {
		return o.operatorRuntime.Open(operator.NewContext(o.logger.Named("operator"),
			o.StoreManager.Controller(o.Name()),
			o.callerChan, operator.NewTimerManager()),
			o.collector)
	}); err != nil {
		return errors.WithMessage(err, "failed to start task")
	}
	o.running = true
	for {
		select {
		case <-o.ctx.Done():
			o.running = false
			if err := o.operatorRuntime.Close(); err != nil {
				return err
			}
			return nil
		case caller := <-o.callerChan:
			caller()
		case e1 := <-o.input1Chan:
			o.barrierAligner.OnElementOrBarrier1(e1)
			bufferSize := len(o.input1Chan)
			for i := 0; i < bufferSize; i++ {
				o.barrierAligner.OnElementOrBarrier1(<-o.input1Chan)
			}
		case e2 := <-o.input2Chan:
			o.barrierAligner.OnElementOrBarrier2(e2)
			bufferSize := len(o.input1Chan)
			for i := 0; i < bufferSize; i++ {
				o.barrierAligner.OnElementOrBarrier2(<-o.input2Chan)
			}
		}
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) Running() bool {
	return o.running
}

func (o *OperatorTask[IN1, IN2, OUT]) Emit1(e1 ElementOrBarrier) {
	o.input1Chan <- e1

}

func (o *OperatorTask[IN1, IN2, OUT]) Emit2(e2 ElementOrBarrier) {
	o.input2Chan <- e2
}

// -------------------------------------Processor---------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) OnElement1(e1 element.Element[IN1]) {
	for _, listener := range o.ElementListeners {
		listener.NotifyInput1(e1)
	}
	switch e1.Type() {
	case element.EventElement:
		o.operatorRuntime.ProcessEvent1(e1.AsEvent())
	case element.WatermarkElement:
		o.operatorRuntime.ProcessWatermark1(e1.AsWatermark())
	case element.WatermarkStatusElement:
		o.operatorRuntime.ProcessWatermarkStatus1(e1.AsWatermarkStatus())
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) OnElement2(e2 element.Element[IN2]) {
	for _, listener := range o.ElementListeners {
		listener.NotifyInput2(e2)
	}
	switch e2.Type() {
	case element.EventElement:
		o.operatorRuntime.ProcessEvent2(e2.AsEvent())
	case element.WatermarkElement:
		o.operatorRuntime.ProcessWatermark2(e2.AsWatermark())
	case element.WatermarkStatusElement:
		o.operatorRuntime.ProcessWatermarkStatus2(e2.AsWatermarkStatus())
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) MutexOnElement(e ElementOrBarrier) {
	o.rwMutex.RLock()
	o.OnElementOrBarrier(e)
	o.rwMutex.RUnlock()
}

func (o *OperatorTask[IN1, IN2, OUT]) OnElementOrBarrier(e ElementOrBarrier) {
	o.Emit(e)
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) TriggerBarrier(barrier Barrier) {
	o.rwMutex.Lock()
	o.collector.Emit = o.MutexOnElement
	o.OnElementOrBarrier(ElementOrBarrier{Upstream: o.Name(), Value: barrier})
	o.NotifyBarrierCome(barrier)
	o.rwMutex.Unlock()
	o.collector.Emit = o.OnElementOrBarrier
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

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierCome(barrier Barrier) {
	o.operatorRuntime.NotifyCheckpointCome(barrier.Id)
}

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierComplete(barrier Barrier) {
	o.operatorRuntime.NotifyCheckpointComplete(barrier.Id)
	switch barrier.BarrierType {
	case ExitpointBarrier:
		o.cancelFunc()
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierCancel(barrier Barrier) {
	o.operatorRuntime.NotifyCheckpointCancel(barrier.Id)

}

func New[IN1, IN2, OUT any](options Options[IN1, IN2, OUT]) *OperatorTask[IN1, IN2, OUT] {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &OperatorTask[IN1, IN2, OUT]{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		logger:     log.Global().Named(options.Name + ".task"),
		Options:    options,
		rwMutex:    &sync.RWMutex{},
		callerChan: make(chan func()),
		running:    false,
	}
}
