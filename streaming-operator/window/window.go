package window

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	"math"
	"sync"

	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/pkg/errors"
)

type operator[KEY comparable, IN, ACC, WIN, OUT any] struct {
	BaseOperator[IN, any, OUT]
	SelectorFn[KEY, IN]
	TriggerFn[KEY, IN]
	AssignerFn[KEY, IN]
	ProcessWindowFn[KEY, WIN, OUT]
	AggregatorFn[IN, ACC, WIN]
	AllowedLateness int64
	timerService    *TimerService[KeyAndWindow[KEY]]

	windowCtx  WContext[KEY]
	state      *map[Window]map[KEY]ACC
	stateMutex *sync.RWMutex
	nilACC     ACC
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	if err := a.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	if state, stateMutex, err := store.RegisterOrGet[map[Window]map[KEY]ACC](ctx.Store(), store.StateDescriptor[map[Window]map[KEY]ACC]{
		Key: "window-aggregate-key-state",
		Initializer: func() map[Window]map[KEY]ACC {
			return map[Window]map[KEY]ACC{}
		},
		Serializer:   nil,
		Deserializer: nil,
	}); err != nil {
		return errors.WithMessage(err, "failed to init window state")
	} else {
		a.state = state
		a.stateMutex = stateMutex
	}
	a.timerService = GetTimerService[KeyAndWindow[KEY]](ctx, "window-timer", a)
	a.windowCtx = &context[KEY]{
		Controller:   a.Ctx.Store(),
		TimerService: a.timerService,
	}
	return nil

}

func (a *operator[KEY, IN, ACC, WIN, OUT]) ProcessEvent1(e *element.Event[IN]) {
	isSkip := true
	key := a.SelectorFn(e.Value)
	windows := a.AssignWindows(a.windowCtx, e.Value, e.Timestamp)
	for _, window := range windows {
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
		win := (*a.state)[timer.Content.window][timer.Content.key]
		outputs := a.ProcessWindowFn.Process(timer.Content.window, timer.Content.key, a.AggregatorFn.GetResult(win))
		for _, out := range outputs {
			a.Collector.EmitEvent(&element.Event[OUT]{
				Value:        out,
				Timestamp:    timer.Content.window.MaxTimestamp(),
				HasTimestamp: true,
			})
		}

	}
	if triggerResult.IsPurge() {
		delete((*a.state)[timer.Content.window], timer.Content.key)
	}
	if !a.AssignerFn.IsEventTime() && a.isCleanupTime(timer.Content.window, timer.Timestamp) {
		delete((*a.state)[timer.Content.window], timer.Content.key)
		if len((*a.state)[timer.Content.window]) == 0 {
			delete(*a.state, timer.Content.window)
		}
		a.TriggerFn.Clear(a.windowCtx, timer)
		a.ProcessWindowFn.Clear(timer.Content.window, timer.Content.key)
	}
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) OnEventTime(timer Timer[KeyAndWindow[KEY]]) {
	triggerResult := a.TriggerFn.OnEventTimer(timer)
	if triggerResult.IsFire() {
		win := (*a.state)[timer.Content.window][timer.Content.key]
		outputs := a.ProcessWindowFn.Process(timer.Content.window, timer.Content.key, a.AggregatorFn.GetResult(win))
		for _, out := range outputs {
			a.Collector.EmitEvent(&element.Event[OUT]{
				Value:        out,
				Timestamp:    timer.Content.window.MaxTimestamp(),
				HasTimestamp: true,
			})
		}

	}
	if triggerResult.IsPurge() {
		delete((*a.state)[timer.Content.window], timer.Content.key)
	}
	if a.AssignerFn.IsEventTime() && a.isCleanupTime(timer.Content.window, timer.Timestamp) {
		delete(*a.state, timer.Content.window)
		a.TriggerFn.Clear(a.windowCtx, timer)
		a.ProcessWindowFn.Clear(timer.Content.window, timer.Content.key)
	}

}

func (a *operator[KEY, IN, ACC, WIN, OUT]) registerCleanupTimer(window Window, key KEY) {
	cleanupTime := a.cleanupTime(window)
	if cleanupTime == math.MaxInt64 {
		return
	}
	timer := Timer[KeyAndWindow[KEY]]{
		Content: KeyAndWindow[KEY]{
			window: window,
			key:    key,
		},
		Timestamp: cleanupTime,
	}
	if a.AssignerFn.IsEventTime() {
		a.timerService.RegisterEventTimeTimer(timer)
	} else {
		a.timerService.RegisterProcessingTimeTimer(timer)
	}
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) isCleanupTime(window Window, timestamp int64) bool {
	return timestamp == a.cleanupTime(window)
}

func (a *operator[KEY, IN, ACC, WIN, OUT]) isEventLate(eventTimestamp int64) bool {
	return (a.AssignerFn.IsEventTime()) &&
		(eventTimestamp+a.AllowedLateness <= a.timerService.CurrentEventTimestamp())
}

// cleanupTime returns the cleanup time for a window,
// which is window.maxTimestamp + allowedLateness.
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

func Aggregate[KEY comparable, IN, OUT any](upstreams []stream.Stream[IN],
	selector SelectorFn[KEY, IN],
	trigger TriggerFn[KEY, IN],
	assigner AssignerFn[KEY, IN],
	aggregator AggregatorFn[IN, OUT, OUT],
	allowedLateness int64,
	name string, applyFns ...stream.WithOperatorStreamOptions[IN, any, OUT]) (*stream.OperatorStream[IN, any, OUT], error) {
	options := stream.ApplyWithOperatorStreamOptionsFns(applyFns)
	options.Name = name
	options.New = func() Operator[IN, any, OUT] {
		return &operator[KEY, IN, OUT, OUT, OUT]{
			BaseOperator:    BaseOperator[IN, any, OUT]{},
			SelectorFn:      selector,
			TriggerFn:       trigger,
			AssignerFn:      assigner,
			AggregatorFn:    aggregator,
			AllowedLateness: allowedLateness,
			ProcessWindowFn: &PassThroughProcessWindowFn[KEY, OUT]{},
		}
	}
	return stream.ApplyOneInput[IN, OUT](upstreams, options)
}
