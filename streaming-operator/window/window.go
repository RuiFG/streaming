package window

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	"math"
	"sync"

	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type operator[KEY comparable, IN, ACC, WIN, OUT any] struct {
	BaseOperator[IN, any, OUT]
	SelectorFn[KEY, IN]
	TriggerFn[KEY, IN]
	AssignerFn[KEY, IN]
	ProcessWindowFn[KEY, WIN, OUT]
	AggregatorFn[IN, ACC, WIN]
	AllowedLateness int64

	timerService *TimerService[KeyAndWindow[KEY]]

	windowCtx  WContext[KEY]
	state      *map[Window]map[KEY]ACC
	stateMutex *sync.RWMutex
	nilACC     ACC
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) Open(ctx Context, collector element.Collector[OUT]) (err error) {
	if err = a.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	if stateController, err := store.GobRegisterOrGet[map[Window]map[KEY]ACC](ctx.Store(), "window-aggregate-Key-state",
		func() map[Window]map[KEY]ACC {
			return map[Window]map[KEY]ACC{}
		}, nil, nil); err != nil {
		return fmt.Errorf("failed to init window state: %w", err)
	} else {
		a.state = stateController.Pointer()
	}
	if a.timerService, err = GetTimerService[KeyAndWindow[KEY]](ctx, "Window-timer", a); err != nil {
		return fmt.Errorf("failed to get operator timer service :%w", err)
	}
	a.windowCtx = &context[KEY]{
		Controller:   a.Ctx.Store(),
		TimerService: a.timerService,
	}
	return nil

}

func (a *operator[KEY, IN, ACC, WIN, OUT]) ProcessEvent(e *element.Event[IN]) {
	isSkip := true
	key := a.SelectorFn(e.Value)
	windows := a.AssignWindows(a.windowCtx, e.Value, e.Timestamp)
	for _, window := range windows {
		if a.isWindowLate(window) {
			continue
		}
		isSkip = false
		if (*a.state)[window] == nil {
			(*a.state)[window] = map[KEY]ACC{}
		}
		(*a.state)[window][key] = a.AggregatorFn.Add((*a.state)[window][key], e.Value)

		triggerResult := a.TriggerFn.OnElement(a.windowCtx, window, key, e.Value)

		if triggerResult.IsFire() {

			win := (*a.state)[window][key]

			outputs := a.ProcessWindowFn.Process(window, key, a.AggregatorFn.GetResult(win))
			for _, out := range outputs {
				a.Collector.EmitEvent(&element.Event[OUT]{
					Value:        out,
					Timestamp:    window.MaxTimestamp(),
					HasTimestamp: true,
				})
			}

		}
		if triggerResult.IsPurge() {
			delete((*a.state)[window], key)
		}
		a.registerCleanupTimer(window, key)
	}

	if isSkip && a.isEventLate(e.Timestamp) {
		fmt.Println("drop event", e.Value)
		//drop event
	}
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) OnProcessingTime(timer Timer[KeyAndWindow[KEY]]) {
	triggerResult := a.TriggerFn.OnProcessingTimer(timer)
	if triggerResult.IsFire() {
		win := (*a.state)[timer.Payload.Window][timer.Payload.Key]
		outputs := a.ProcessWindowFn.Process(timer.Payload.Window, timer.Payload.Key, a.AggregatorFn.GetResult(win))
		for _, out := range outputs {
			a.Collector.EmitEvent(&element.Event[OUT]{
				Value:        out,
				Timestamp:    timer.Payload.Window.MaxTimestamp(),
				HasTimestamp: true,
			})
		}

	}
	if triggerResult.IsPurge() {
		delete((*a.state)[timer.Payload.Window], timer.Payload.Key)
	}
	if !a.AssignerFn.IsEventTime() && a.isCleanupTime(timer.Payload.Window, timer.Timestamp) {
		delete((*a.state)[timer.Payload.Window], timer.Payload.Key)
		if len((*a.state)[timer.Payload.Window]) == 0 {
			delete(*a.state, timer.Payload.Window)
		}
		a.TriggerFn.Clear(a.windowCtx, timer)
		a.ProcessWindowFn.Clear(timer.Payload.Window, timer.Payload.Key)
	}
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) OnEventTime(timer Timer[KeyAndWindow[KEY]]) {
	triggerResult := a.TriggerFn.OnEventTimer(timer)
	if triggerResult.IsFire() {
		win := (*a.state)[timer.Payload.Window][timer.Payload.Key]
		outputs := a.ProcessWindowFn.Process(timer.Payload.Window, timer.Payload.Key, a.AggregatorFn.GetResult(win))
		for _, out := range outputs {
			a.Collector.EmitEvent(&element.Event[OUT]{
				Value:        out,
				Timestamp:    timer.Payload.Window.MaxTimestamp(),
				HasTimestamp: true,
			})
		}

	}
	if triggerResult.IsPurge() {
		delete((*a.state)[timer.Payload.Window], timer.Payload.Key)
	}
	if a.AssignerFn.IsEventTime() && a.isCleanupTime(timer.Payload.Window, timer.Timestamp) {
		delete(*a.state, timer.Payload.Window)
		a.TriggerFn.Clear(a.windowCtx, timer)
		a.ProcessWindowFn.Clear(timer.Payload.Window, timer.Payload.Key)
	}

}

func (a *operator[KEY, IN, ACC, WIN, OUT]) registerCleanupTimer(window Window, key KEY) {
	cleanupTime := a.cleanupTime(window)
	if cleanupTime == math.MaxInt64 {
		return
	}
	timer := Timer[KeyAndWindow[KEY]]{
		Payload: KeyAndWindow[KEY]{
			Window: window,
			Key:    key,
		},
		Timestamp: cleanupTime,
	}
	if a.AssignerFn.IsEventTime() {
		a.timerService.RegisterEventTimeTimer(timer)
	} else {
		a.timerService.RegisterProcessingTimeTimer(timer)
	}
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) isEventLate(eventTimestamp int64) bool {
	return (a.AssignerFn.IsEventTime()) &&
		(eventTimestamp+a.AllowedLateness <= a.timerService.CurrentEventTimestamp())
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) isWindowLate(window Window) bool {
	return (a.AssignerFn.IsEventTime()) &&
		(a.cleanupTime(window) <= a.timerService.CurrentEventTimestamp())
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) isCleanupTime(window Window, timestamp int64) bool {
	return timestamp == a.cleanupTime(window)
}

// cleanupTime returns the cleanup time for a Window,
// which is Window.maxTimestamp + allowedLateness.
// In case this leads to a value greater than math.MaxInt64 then a cleanup time of math.MaxInt64 is returned.
func (a *operator[KEY, IN, ACC, WIN, OUT]) cleanupTime(window Window) int64 {
	if a.AssignerFn.IsEventTime() {
		cleanupTime := window.MaxTimestamp() + a.AllowedLateness
		if cleanupTime >= window.MaxTimestamp() {
			return cleanupTime
		} else {
			return math.MaxInt64
		}
	} else {
		return window.MaxTimestamp()
	}
}

func Apply[KEY comparable, IN, ACC, WIN, OUT any](upstream stream.Stream[IN], name string, withOptionsFns ...WithOptions[KEY, IN, ACC, WIN, OUT]) (stream.Stream[OUT], error) {
	o := &options[KEY, IN, ACC, WIN, OUT]{}
	for _, withOptionsFn := range withOptionsFns {
		if err := withOptionsFn(o); err != nil {
			return nil, fmt.Errorf("%s illegal parameter: %w", name, err)
		}
	}
	return stream.ApplyOneInput[IN, OUT](upstream, stream.OperatorStreamOptions{
		Name: name,
		Operator: OneInputOperatorToOperator[IN, OUT](&operator[KEY, IN, ACC, WIN, OUT]{
			BaseOperator:    BaseOperator[IN, any, OUT]{},
			SelectorFn:      o.selectorFn,
			TriggerFn:       o.triggerFn,
			AssignerFn:      o.assignerFn,
			AggregatorFn:    o.aggregatorFn,
			AllowedLateness: o.allowedLateness,
			ProcessWindowFn: o.processWindowFn,
		}),
	})
}
