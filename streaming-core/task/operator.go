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

type OperatorOptions[IN1, IN2, OUT any] struct {
	NameSuffix  string
	qos         string
	Handlers    []ElementHandler[IN1, IN2]
	New         component.NewOperator[IN1, IN2, OUT]
	Channel     bool
	ChannelSize uint
}

func (o OperatorOptions[IN1, IN2, OUT]) Name() string {
	return "operator." + o.NameSuffix
}

type OperatorTask[IN1, IN2, OUT any] struct {
	OperatorOptions[IN1, IN2, OUT]
	service.TimeScheduler
	ctx                  _c.Context
	running              bool
	emitNext             element.EmitNext[OUT]
	operator             component.Operator[IN1, IN2, OUT]
	synchronousProcessor SynchronousProcessor[IN1, IN2]
	storeManger          store.Manager

	inputCount int
	cleanFns   []func()

	barrierSignalChan chan barrier.Signal

	elementMeta element.Meta
}

func (o *OperatorTask[IN1, IN2, OUT]) Daemon() error {
	o.running = true
	o.operator = o.New()
	o.elementMeta = element.Meta{Partition: 0, Upstream: o.Name()}
	var elementHandler ElementHandler[IN1, IN2]
	switch o.qos {
	case "1":
		elementHandler = &TrackerHandler[IN1, IN2]{
			handlers:       append(o.Handlers, o),
			Trigger:        o,
			inputCount:     o.inputCount,
			pendingBarrier: map[barrier.Detail]int{},
		}
	case "2":
		elementHandler = &AlignerHandler[IN1, IN2]{
			handlers:        append(o.Handlers, o),
			Trigger:         o,
			inputCount:      o.inputCount,
			blockedUpstream: map[string]bool{},
		}
	}
	if o.Channel {
		syncProcessor := newChannelSyncProcessor[IN1, IN2](elementHandler, o.ChannelSize)
		syncProcessor.Start()
		o.cleanFns = append(o.cleanFns, syncProcessor.Stop)
		o.synchronousProcessor = syncProcessor
	} else {
		o.synchronousProcessor = newMutexSyncProcess[IN1, IN2](elementHandler)
	}
	if err := safe.Run(func() error {
		return o.operator.Open(component.NewContext(o.ctx, o.Name()),
			element.Collector[OUT]{Meta: o.elementMeta, EmitNext: o.emitNext})
	}); err != nil {
		return errors.WithMessage(err, "failed to start operator task")
	}
	return nil
}

func (o *OperatorTask[IN1, IN2, OUT]) Running() bool {
	return o.running
}

func (o *OperatorTask[IN1, IN2, OUT]) Emit1(e1 element.Element[IN1]) {
	o.synchronousProcessor.ProcessElement1(e1)
}
func (o *OperatorTask[IN1, IN2, OUT]) Emit2(e2 element.Element[IN2]) {
	o.synchronousProcessor.ProcessElement2(e2)
}

// -------------------------------------ElementHandler---------------------------------------------

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

func (o *OperatorTask[IN1, IN2, OUT]) TriggerBarrier(detail barrier.Detail) {
	o.emitNext(&element.Barrier[OUT]{Meta: o.elementMeta, Detail: detail})
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

func (o *OperatorTask[IN1, IN2, OUT]) NotifyComplete(detail barrier.Detail) {
	o.operator.NotifyComplete(detail)
	switch detail.Type {
	case barrier.ExitpointBarrier:
		if err := o.operator.Close(); err != nil {
			//todo handler error
		}
		for _, fn := range o.cleanFns {
			fn()
		}
		o.running = false
	}
}

func (o *OperatorTask[IN1, IN2, OUT]) NotifyCancel(detail barrier.Detail) {
	o.operator.NotifyCancel(detail)

}

// --------------------------------------timeScheduler--------------------------------------------

func (o *OperatorTask[IN1, IN2, OUT]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: o.synchronousProcessor.ProcessCaller})
}

func (o *OperatorTask[IN1, IN2, OUT]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	o.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: o.synchronousProcessor.ProcessCaller})
}

func NewOperatorTask[IN1, IN2, OUT any](ctx _c.Context, options OperatorOptions[IN1, IN2, OUT], emitNext element.EmitNext[OUT]) *OperatorTask[IN1, IN2, OUT] {
	return &OperatorTask[IN1, IN2, OUT]{
		OperatorOptions: options,
		TimeScheduler:   service.NewTimeSchedulerService(ctx),
		ctx:             ctx,
		running:         false,
		emitNext:        emitNext,
		cleanFns:        []func(){},
	}
}
