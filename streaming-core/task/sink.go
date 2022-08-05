package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/barrier"
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/safe"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"time"
)

type SinkOptions[IN any] struct {
	NameSuffix  string
	New         component.NewSink[IN]
	Channel     bool
	ChannelSize uint
	qos         string
	inputCount  int
	Handlers    []ElementHandler[IN, any]
}

func (receiver SinkOptions[IN]) Name() string {
	return "sink." + receiver.NameSuffix
}

type SinkTask[IN any] struct {
	ctx _c.Context
	SinkOptions[IN]
	running bool
	channel chan element.Element[IN]
	sink    component.Sink[IN]

	storeManger       store.Manager
	barrierSignalChan chan barrier.Signal
	cleanFns          []func()
	service.TimeScheduler
	synchronousProcessor SynchronousProcessor[IN, any]
	elementMeta          element.Meta
}

func (o *SinkTask[IN]) Daemon() error {
	o.running = true
	o.sink = o.New()
	o.elementMeta = element.Meta{Partition: 0, Upstream: o.Name()}
	var elementHandler ElementHandler[IN, any]
	switch o.qos {
	case "1":
		elementHandler = &TrackerHandler[IN, any]{
			handlers:       append(o.Handlers, o),
			Trigger:        o,
			inputCount:     o.inputCount,
			pendingBarrier: map[barrier.Detail]int{},
		}
	case "2":
		elementHandler = &AlignerHandler[IN, any]{
			handlers:        append(o.Handlers, o),
			Trigger:         o,
			inputCount:      o.inputCount,
			blockedUpstream: map[string]bool{},
		}
	}
	if o.Channel {
		syncProcessor := newChannelSyncProcessor[IN, any](elementHandler, o.ChannelSize)
		syncProcessor.Start()
		o.cleanFns = append(o.cleanFns, syncProcessor.Stop)
		o.synchronousProcessor = syncProcessor
	} else {
		o.synchronousProcessor = newMutexSyncProcess[IN, any](elementHandler)
	}
	if err := safe.Run(func() error {
		return o.sink.Open(component.NewContext(o.ctx, o.Name()))
	}); err != nil {
		return errors.WithMessage(err, "failed to start operator task")
	}
	return nil
}

func (o *SinkTask[IN]) Running() bool {
	return o.running
}

func (o *SinkTask[IN]) Emit(e element.Element[IN]) {
	o.synchronousProcessor.ProcessElement1(e)
}

// -------------------------------------ElementHandler---------------------------------------------

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

func (o *SinkTask[IN]) TriggerBarrier(detail barrier.Detail) {
	message := barrier.ACK
	safe.Go(func() error {
		if err := o.storeManger.Save(detail.Id); err != nil {
			message = barrier.DEC
		}
		o.barrierSignalChan <- barrier.Signal{
			Name:    o.Name(),
			Message: message,
			Detail:  detail}
		return nil
	})

}

// -------------------------------------BarrierListener------------------------------------------

func (o *SinkTask[IN]) NotifyComplete(detail barrier.Detail) {
	o.sink.NotifyComplete(detail)
	switch detail.Type {
	case barrier.ExitpointBarrier:
		if err := o.sink.Close(); err != nil {
			//todo handler error
		}
		for _, fn := range o.cleanFns {
			fn()
		}
		o.running = false
	}
}

func (o *SinkTask[IN]) NotifyCancel(detail barrier.Detail) {
	o.sink.NotifyCancel(detail)
}

// --------------------------------------timeScheduler--------------------------------------------

func (o *SinkTask[IN]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: o.synchronousProcessor.ProcessCaller})
}

func (o *SinkTask[IN]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: o.synchronousProcessor.ProcessCaller})
}

func NewSinkTask[IN any](ctx _c.Context, options SinkOptions[IN]) *SinkTask[IN] {
	return &SinkTask[IN]{ctx: ctx, SinkOptions: options}
}
