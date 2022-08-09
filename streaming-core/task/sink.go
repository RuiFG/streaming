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

type SinkOptions[IN any] struct {
	Options
	New          component.NewSink[IN]
	InputHandler element.InputHandler[IN, any]
}

func (receiver SinkOptions[IN]) Name() string {
	return "sink." + receiver.NameSuffix
}

type SinkTask[IN any] struct {
	ctx        _c.Context
	cancelFunc _c.CancelFunc
	SinkOptions[IN]
	service.TimeScheduler
	*mutex[IN, any, any]

	running bool
	sink    component.Sink[IN]
}

func (o *SinkTask[IN]) Daemon() error {
	o.running = true
	o.sink = o.New()
	var inputHandler element.InputHandler[IN, any]
	if o.SinkOptions.InputHandler == nil {
		inputHandler = o
	} else {
		inputHandler = element.InputHandlerChain[IN, any]{o.SinkOptions.InputHandler, o}
	}
	switch o.Options.QOS {
	case AtMostOnce, AtLeastOnce:
		inputHandler = NewTrackerBridge(
			inputHandler, o, o.Options.InputCount)
	case ExactlyOnce:
		inputHandler = NewAlignerBridge(
			inputHandler, o, o.Options.InputCount)
	}
	o.mutex = newMutexSyncProcess[IN, any, any](inputHandler, nil)

	if err := safe.Run(func() error {
		return o.sink.Open(component.NewContext(o.ctx, o.StoreManager.Controller(o.Name()), o, log.Named(o.Name())))
	}); err != nil {
		return errors.WithMessage(err, "failed to start operator task")
	}
	return nil
}

func (o *SinkTask[IN]) Running() bool {
	return o.running
}

func (o *SinkTask[IN]) Emit(e element.Element[IN]) {
	o.mutex.ProcessInput1(e)
}

// -------------------------------------InputHandler---------------------------------------------

func (o *SinkTask[IN]) OnElement1(e1 element.Element[IN]) {
	switch e1.Type() {
	case element.EventElement:
		o.sink.ProcessEvent(e1.AsEvent())
	case element.WatermarkElement:
		o.sink.ProcessWatermark(e1.AsWatermark())
	}
}

func (o *SinkTask[IN]) OnElement2(element.Element[any]) { panic("not implement me") }

// -------------------------------------BarrierTrigger---------------------------------------------

func (o *SinkTask[IN]) TriggerBarrier(detail element.Detail) {

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

func (o *SinkTask[IN]) NotifyBarrierCome(detail element.Detail) {
	o.sink.NotifyBarrierCome(detail)
}

func (o *SinkTask[IN]) NotifyBarrierComplete(detail element.Detail) {
	o.sink.NotifyBarrierComplete(detail)
	switch detail.BarrierType {
	case element.ExitpointBarrier:
		if err := o.sink.Close(); err != nil {
			//todo handler error
		}
		o.running = false
	}
}

func (o *SinkTask[IN]) NotifyBarrierCancel(detail element.Detail) {
	o.sink.NotifyBarrierCancel(detail)
}

// --------------------------------------timeScheduler--------------------------------------------

func (o *SinkTask[IN]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: o.mutex.ProcessCaller})
}

func (o *SinkTask[IN]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: o.mutex.ProcessCaller})
}

func NewSinkTask[IN any](options SinkOptions[IN]) *SinkTask[IN] {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &SinkTask[IN]{
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		SinkOptions:   options,
		TimeScheduler: service.NewTimeSchedulerService(ctx),
	}
}
