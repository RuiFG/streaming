package service

import (
	_c "context"
	"fmt"
	"reflect"
	"streaming/streaming-core/pkg/safe"
	"time"
)

type TimeCallback interface {
	OnProcessingTime(processingTime time.Time)
}

type TimeScheduler interface {
	RegisterTicker(duration time.Duration, callback TimeCallback)
	RegisterTimer(duration time.Duration, callback TimeCallback)
	Quiesce()
	Errors() <-chan error
}

type defaultTimeScheduler struct {
	ctx       _c.Context
	cancel    _c.CancelFunc
	errorChan chan error
}

func (d *defaultTimeScheduler) Errors() <-chan error {
	return d.errorChan
}

func (d *defaultTimeScheduler) Quiesce() {
	select {
	case <-d.ctx.Done():
	default:
		d.cancel()
		close(d.errorChan)
	}
}

func (d *defaultTimeScheduler) RegisterTicker(duration time.Duration, callback TimeCallback) {
	select {
	case <-d.ctx.Done():
	default:
		ticker := time.NewTicker(duration)
		safe.GoChannelWithMessage(func() error {
			select {
			case pc := <-ticker.C:
				callback.OnProcessingTime(pc)
				return nil
			case <-d.ctx.Done():
				ticker.Stop()
				return nil
			}
		}, fmt.Sprintf("%s scheduler is quiesce", reflect.TypeOf(callback).Name()), d.errorChan)
	}

}

func (d *defaultTimeScheduler) RegisterTimer(duration time.Duration, callback TimeCallback) {
	select {
	case <-d.ctx.Done():
	default:
		timer := time.NewTimer(duration)
		safe.GoChannelWithMessage(func() error {
			for {
				select {
				case pc := <-timer.C:
					callback.OnProcessingTime(pc)
				case <-d.ctx.Done():
					timer.Stop()
					return nil
				}
			}
		}, fmt.Sprintf("%s scheduler is quiesce", reflect.TypeOf(callback).Name()), d.errorChan)
	}
}

func NewTimeSchedulerService(ctx _c.Context) TimeScheduler {
	cancel, cancelFunc := _c.WithCancel(ctx)
	return &defaultTimeScheduler{cancel: cancelFunc, ctx: cancel, errorChan: make(chan error)}
}
