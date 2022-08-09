package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/pkg/errors"
	"time"
)

type SourceOptions[OUT any] struct {
	Options
	OutputHandler element.OutputHandler[OUT]
	New           component.NewSource[OUT]
}

func (receiver SourceOptions[OUT]) Name() string {
	return "source." + receiver.NameSuffix
}

type SourceTask[OUT any] struct {
	ctx        _c.Context
	cancelFunc _c.CancelFunc
	SourceOptions[OUT]
	service.TimeScheduler
	*mutex[any, any, OUT]

	running     bool
	source      component.Source[OUT]
	elementMeta element.Meta
}

func (s *SourceTask[OUT]) Daemon() error {
	s.running = true
	s.source = s.SourceOptions.New()
	s.elementMeta = element.Meta{Partition: 0, Upstream: s.Name()}
	s.mutex = newMutexSyncProcess[any, any, OUT](nil, s.SourceOptions.OutputHandler)
	if err := safe.Run(func() error {
		return s.source.Open(component.NewContext(s.ctx, s.StoreManager.Controller(s.Name()), s, log.Named(s.Name())),
			element.Collector[OUT]{Meta: s.elementMeta, Emit: s.SourceOptions.OutputHandler.OnElement})
	}); err != nil {
		return errors.WithMessage(err, "failed to start operator task")
	}
	safe.Go(func() error {
		s.source.Run()
		return nil
	})
	return nil
}

func (s *SourceTask[OUT]) Running() bool {
	return s.running
}

// -------------------------------------BarrierTrigger---------------------------------------------

func (s *SourceTask[OUT]) TriggerBarrier(detail element.Detail) {
	s.mutex.ProcessCaller(func() {
		s.OutputHandler.OnElement(&element.Barrier[OUT]{Meta: s.elementMeta, Detail: detail})
		s.NotifyBarrierCome(detail)
		message := element.ACK
		if err := s.StoreManager.Save(detail.Id); err != nil {
			message = element.DEC
		}
		s.BarrierSignalChan <- element.Signal{
			Name:    s.Name(),
			Message: message,
			Detail:  detail}
	})
}

// -------------------------------------BarrierListener------------------------------------------

func (s *SourceTask[OUT]) NotifyBarrierCome(detail element.Detail) {
	s.source.NotifyBarrierCome(detail)
}

func (s *SourceTask[OUT]) NotifyBarrierComplete(detail element.Detail) {
	s.source.NotifyBarrierComplete(detail)
	switch detail.BarrierType {
	case element.ExitpointBarrier:
		if err := s.source.Close(); err != nil {
			//todo handler error
		}
		s.running = false
	}
}

func (s *SourceTask[OUT]) NotifyBarrierCancel(detail element.Detail) {
	s.source.NotifyBarrierCancel(detail)
}

// --------------------------------------timeScheduler--------------------------------------------

func (s *SourceTask[OUT]) RegisterTicker(duration time.Duration, cb service.TimeCallback) {
	s.TimeScheduler.RegisterTicker(duration, &callbackAgent{cb: cb, agent: s.mutex.ProcessCaller})
}

func (s *SourceTask[OUT]) RegisterTimer(duration time.Duration, cb service.TimeCallback) {
	s.TimeScheduler.RegisterTimer(duration, &callbackAgent{cb: cb, agent: s.mutex.ProcessCaller})
}

func NewSourceTask[OUT any](options SourceOptions[OUT]) *SourceTask[OUT] {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &SourceTask[OUT]{
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		SourceOptions: options,
		TimeScheduler: service.NewTimeSchedulerService(ctx),
		running:       false,
	}
}
