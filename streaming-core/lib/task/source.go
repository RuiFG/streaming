package task

import (
	_c "context"
	"github.com/pkg/errors"
	"streaming/streaming-core/lib/barrier"
	component2 "streaming/streaming-core/lib/component"
	element2 "streaming/streaming-core/lib/element"
	"streaming/streaming-core/lib/service"
	"streaming/streaming-core/lib/store"
	"streaming/streaming-core/pkg/safe"
	"time"
)

type SourceOptions[OUT any] struct {
	NameSuffix string
	New        component2.NewSource[OUT]
}

func (receiver SourceOptions[OUT]) Name() string {
	return "source." + receiver.NameSuffix
}

type SourceTask[OUT any] struct {
	SourceOptions[OUT]
	ctx _c.Context
	service.TimeScheduler
	synchronousProcessor SynchronousProcessor[any, any]
	running              bool

	emitNext element2.EmitNext[OUT]
	source   component2.Source[OUT]

	elementMeta       element2.Meta
	storeManager      store.Manager
	barrierSignalChan chan barrier.Signal
	cleanFns          []func()
}

func (s *SourceTask[OUT]) Daemon() error {
	s.running = true
	s.source = s.New()
	if err := safe.Run(func() error {
		return s.source.Open(component2.NewContext(s.ctx, s.Name()), element2.Collector[OUT]{Meta: element2.Meta{Partition: 0, Upstream: s.Name()}, EmitNext: s.emitNext})
	}); err != nil {
		return errors.WithMessage(err, "failed to start source task")
	}

	return nil
}

func (s *SourceTask[OUT]) Running() bool {
	return s.running
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (s *SourceTask[OUT]) TriggerBarrier(detail barrier.Detail) {
	s.emitNext(&element2.Barrier[OUT]{Meta: s.elementMeta, Detail: detail})
	message := barrier.ACK
	safe.Go(func() error {
		if err := s.storeManager.Save(detail.Id); err != nil {
			message = barrier.DEC
		}
		s.barrierSignalChan <- barrier.Signal{
			Name:    s.Name(),
			Message: message,
			Detail:  detail}
		return nil
	})

}

// -------------------------------------BarrierListener------------------------------------------

func (s *SourceTask[OUT]) NotifyComplete(detail barrier.Detail) {
	s.source.NotifyComplete(detail)
	switch detail.Type {
	case barrier.ExitpointBarrier:
		if err := s.source.Close(); err != nil {
			//todo handler error
		}
		for _, fn := range s.cleanFns {
			fn()
		}
		s.running = false
	}
}

func (s *SourceTask[OUT]) NotifyCancel(detail barrier.Detail) {
	s.source.NotifyCancel(detail)
}

// --------------------------------------timeScheduler--------------------------------------------

func (s *SourceTask[OUT]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	s.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: s.synchronousProcessor.ProcessCaller})
}

func (s *SourceTask[OUT]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	s.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: s.synchronousProcessor.ProcessCaller})
}

func NewSourceTask[OUT any](ctx _c.Context, options SourceOptions[OUT], emitNext element2.EmitNext[OUT]) *SourceTask[OUT] {
	return &SourceTask[OUT]{ctx: ctx, SourceOptions: options, emitNext: emitNext}
}
