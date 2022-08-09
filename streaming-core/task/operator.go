package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/safe"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/pkg/errors"
	"time"
)

type OperatorOptions[IN1, IN2, OUT any] struct {
	Options
	InputHandler  element.InputHandler[IN1, IN2]
	OutputHandler element.OutputHandler[OUT]
	New           component.NewOperator[IN1, IN2, OUT]
}

func (o OperatorOptions[IN1, IN2, OUT]) Name() string {
	return "operator." + o.NameSuffix
}

type OperatorTask[IN1, IN2, OUT any] struct {
	ctx        _c.Context
	cancelFunc _c.CancelFunc
	OperatorOptions[IN1, IN2, OUT]
	service.TimeScheduler
	*mutex[IN1, IN2, OUT]

	running     bool
	operator    component.Operator[IN1, IN2, OUT]
	elementMeta element.Meta
}

func (o *OperatorTask[IN1, IN2, OUT]) Daemon() error {
	o.running = true
	o.operator = o.New()
	o.elementMeta = element.Meta{Partition: 0, Upstream: o.Name()}
	var inputHandler element.InputHandler[IN1, IN2]
	if o.OperatorOptions.InputHandler == nil {
		inputHandler = o
	} else {
		inputHandler = element.InputHandlerChain[IN1, IN2]{o.OperatorOptions.InputHandler, o}
	}
	switch o.Options.QOS {
	case AtMostOnce, AtLeastOnce:
		inputHandler = NewTrackerBridge(
			inputHandler, o, o.Options.InputCount)
	case ExactlyOnce:
		inputHandler = NewAlignerBridge(
			inputHandler, o, o.Options.InputCount)
	}
	o.mutex = newMutexSyncProcess(inputHandler, o.OperatorOptions.OutputHandler)
	if err := safe.Run(func() error {
		return o.operator.Open(component.NewContext(o.ctx, o.StoreManager.Controller(o.Name()), o, log.Named(o.Name())),
			element.Collector[OUT]{Meta: o.elementMeta, Emit: o.OperatorOptions.OutputHandler.OnElement})
	}); err != nil {
		return errors.WithMessage(err, "failed to start operator task")
	}
	return nil
}

func (o *OperatorTask[IN1, IN2, OUT]) Running() bool {
	return o.running
}

func (o *OperatorTask[IN1, IN2, OUT]) Emit1(e1 element.Element[IN1]) {
	o.mutex.ProcessInput1(e1)
}
func (o *OperatorTask[IN1, IN2, OUT]) Emit2(e2 element.Element[IN2]) {
	o.mutex.ProcessInput2(e2)
}

// -------------------------------------InputHandler---------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) OnElement1(e1 element.Element[IN1]) {
	switch e1.Type() {
	case element.EventElement:
		o.operator.ProcessEvent1(e1.AsEvent())
	case element.WatermarkElement:
		o.operator.ProcessWatermark1(e1.AsWatermark())
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) OnElement2(e2 element.Element[IN2]) {
	switch e2.Type() {
	case element.EventElement:
		o.operator.ProcessEvent2(e2.AsEvent())
	case element.WatermarkElement:
		o.operator.ProcessWatermark2(e2.AsWatermark())
	}
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) TriggerBarrier(detail element.Detail) {
	o.OutputHandler.OnElement(&element.Barrier[OUT]{Meta: o.elementMeta, Detail: detail})
	o.NotifyBarrierCome(detail)
	message := element.ACK
	if err := o.StoreManager.Save(detail.Id); err != nil {
		message = element.DEC
	}
	o.BarrierSignalChan <- element.Signal{
		Name:    o.Name(),
		Message: message,
		Detail:  detail}

}

// -------------------------------------BarrierListener------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierCome(detail element.Detail) {
	o.operator.NotifyBarrierCome(detail)
}

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierComplete(detail element.Detail) {
	o.operator.NotifyBarrierComplete(detail)
	switch detail.BarrierType {
	case element.ExitpointBarrier:
		if err := o.operator.Close(); err != nil {
			//todo handler error
		}
		o.running = false
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) NotifyBarrierCancel(detail element.Detail) {
	o.operator.NotifyBarrierCancel(detail)

}

// --------------------------------------timeScheduler--------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: o.mutex.ProcessCaller})
}

func (o *OperatorTask[IN1, IN2, OUT]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: o.mutex.ProcessCaller})
}

func NewOperatorTask[IN1, IN2, OUT any](options OperatorOptions[IN1, IN2, OUT]) *OperatorTask[IN1, IN2, OUT] {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &OperatorTask[IN1, IN2, OUT]{
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		OperatorOptions: options,
		TimeScheduler:   service.NewTimeSchedulerService(ctx),
		running:         false,
	}
}
