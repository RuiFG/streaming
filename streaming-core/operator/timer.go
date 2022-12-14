package operator

import (
	"container/heap"
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/store"
	"math"
	"time"
)

// TimerTrigger will be triggered passively as time goes by
type TimerTrigger[T comparable] interface {
	OnProcessingTime(timer Timer[T])
	OnEventTime(timer Timer[T])
}

// Timer is a structure that contains triggering events
type Timer[T comparable] struct {
	Payload   T
	Timestamp int64
}

// timerQueue is a priority queue,
// sorted from smallest to largest according to Timer.Timestamp,
// and use dedupeMap to prevent the same Timer from being inserted.
// If timestamps are inserted in this order
// +---+     +---+     +---+     +---+     +-------------+     +---+
// | 2 | --> | 5 | --> | 3 | --> | 1 | --> | duplicate:3 | --> | 7 |
// +---+     +---+     +---+     +---+     +-------------+     +---+
// Items:
// +---+     +---+     +---+     +---+     +---+
// | 1 | --> | 2 | --> | 3 | --> | 5 | --> | 7 |
// +---+     +---+     +---+     +---+     +---+
type timerQueue[T comparable] struct {
	//exposing is for serialization
	Items     []Timer[T]
	dedupeMap map[Timer[T]]struct{}
	nil       Timer[T]
}

//---------------------------------------------------------------------------------
//Warning: Do not call directly, expose the function only for the heap package to use
//---------------------------------------------------------------------------------

func (t *timerQueue[T]) Less(i, j int) bool {
	return t.Items[i].Timestamp < t.Items[j].Timestamp
}

func (t *timerQueue[T]) Swap(i, j int) {
	t.Items[i], t.Items[j] = t.Items[j], t.Items[i]

}

func (t *timerQueue[T]) Push(x any) {
	item := x.(Timer[T])
	t.Items = append(t.Items, item)
}

func (t *timerQueue[T]) Pop() any {
	old := t.Items
	n := len(old)
	x := old[n-1]
	t.Items = old[0 : n-1]
	return x
}

//---------------------------------------------------------------------------------

func (t *timerQueue[T]) Len() int {
	return len(t.Items)
}

func (t *timerQueue[T]) PushTimer(item Timer[T]) {
	if _, ok := t.dedupeMap[item]; !ok {
		t.dedupeMap[item] = struct{}{}
		heap.Push(t, item)
	}
}

func (t *timerQueue[T]) PopTimer() Timer[T] {
	if len(t.Items) == 0 {
		return t.nil
	} else {
		item := heap.Pop(t).(Timer[T])
		delete(t.dedupeMap, item)
		return item
	}
}

func (t *timerQueue[T]) PeekTimer() Timer[T] {
	return t.Items[0]
}

func (t *timerQueue[T]) Remove(timer Timer[T]) bool {
	index := t.Index(timer)
	if index != -1 {
		delete(t.dedupeMap, timer)
		heap.Remove(t, index)
		if index == 0 {
			return true
		}
	}
	return false
}

func (t *timerQueue[T]) Index(timer Timer[T]) int {
	for index, item := range t.Items {
		if item == timer {
			return index
		}
	}
	return -1
}

type TimerService[T comparable] struct {
	ctx       Context
	trigger   TimerTrigger[T]
	nextTimer *time.Timer

	CurrentWatermarkTimestamp int64
	ProcessTimeCallbackQueue  *timerQueue[T]
	EventTimeCallbackQueue    *timerQueue[T]
}

func (d *TimerService[T]) CurrentProcessingTimestamp() int64 {
	return time.Now().UnixMilli()
}

func (d *TimerService[T]) CurrentEventTimestamp() int64 {
	return d.CurrentWatermarkTimestamp
}

func (d *TimerService[T]) RegisterEventTimeTimer(timer Timer[T]) {
	d.EventTimeCallbackQueue.PushTimer(timer)
}

func (d *TimerService[T]) RegisterProcessingTimeTimer(timer Timer[T]) {
	var nextTriggerTimestamp int64 = math.MaxInt64
	if d.ProcessTimeCallbackQueue.Len() > 0 {
		oldHead := d.ProcessTimeCallbackQueue.PeekTimer()
		d.ProcessTimeCallbackQueue.PushTimer(timer)
		if oldHead != d.ProcessTimeCallbackQueue.PeekTimer() {
			nextTriggerTimestamp = d.ProcessTimeCallbackQueue.PeekTimer().Timestamp
		}
	} else {
		d.ProcessTimeCallbackQueue.PushTimer(timer)
	}
	if timer.Timestamp < nextTriggerTimestamp {
		if d.nextTimer != nil {
			if !d.nextTimer.Stop() {
				//timer has been triggered.
			}
		}
		//
		duration := time.Duration(math.Max(float64(timer.Timestamp-time.Now().UnixMilli()), 0)) * time.Millisecond
		d.nextTimer = time.AfterFunc(duration, func() {
			d.advanceProcessingTimestamp(timer.Timestamp)
		})
	}

}

func (d *TimerService[T]) DeleteProcessingTimeTimer(timer Timer[T]) {
	d.ProcessTimeCallbackQueue.Remove(timer)
}

func (d *TimerService[T]) DeleteEventTimeTimer(timer Timer[T]) {
	d.EventTimeCallbackQueue.Remove(timer)
}

func (d *TimerService[T]) startAdvanceProcessingTimestamp() {
	if d.ProcessTimeCallbackQueue.Len() > 0 && d.nextTimer == nil {
		d.advanceProcessingTimestamp(0)
	}
}

func (d *TimerService[T]) advanceWatermarkTimestamp(timestamp int64) {
	d.CurrentWatermarkTimestamp = timestamp
	for d.EventTimeCallbackQueue.Len() > 0 &&
		d.EventTimeCallbackQueue.PeekTimer().Timestamp <= d.CurrentWatermarkTimestamp {
		d.trigger.OnEventTime(d.EventTimeCallbackQueue.PopTimer())
	}
}

func (d *TimerService[T]) advanceProcessingTimestamp(timestamp int64) {
	//if processing timestamp, the agent gives it to task to execute
	d.ctx.Exec(func() {
		for d.ProcessTimeCallbackQueue.Len() > 0 &&
			d.ProcessTimeCallbackQueue.PeekTimer().Timestamp <= timestamp {
			d.trigger.OnProcessingTime(d.ProcessTimeCallbackQueue.PopTimer())
		}
		if d.ProcessTimeCallbackQueue.Len() > 0 {
			timer := d.ProcessTimeCallbackQueue.PeekTimer()
			duration := time.Duration(math.Max(float64(timer.Timestamp-time.Now().UnixMilli()), 0)) * time.Millisecond
			d.nextTimer = time.AfterFunc(duration, func() {
				d.advanceProcessingTimestamp(timer.Timestamp)
			})
		}
	})

}

type WatermarkTimerAdvances interface {
	advanceWatermarkTimestamp(timestamp int64)
}

type TimerManager struct {
	services map[string]WatermarkTimerAdvances
}

func (t *TimerManager) addWatermarkTimerAdvances(name string, advances WatermarkTimerAdvances) {
	t.services[name] = advances
}

func (t *TimerManager) deleteWatermarkTimerAdvances(name string) {
	delete(t.services, name)
}

func (t *TimerManager) advanceWatermarkTimestamp(timestamp int64) {
	for _, service := range t.services {
		service.advanceWatermarkTimestamp(timestamp)
	}
}

func NewTimerManager() *TimerManager {
	return &TimerManager{services: map[string]WatermarkTimerAdvances{}}
}

func GetTimerService[T comparable](ctx Context, name string, trigger TimerTrigger[T]) (*TimerService[T], error) {

	if timerServiceStateController, err := store.GobRegisterOrGet(ctx.Store(), name, func() TimerService[T] {
		service := TimerService[T]{
			ctx:                       ctx,
			trigger:                   trigger,
			nextTimer:                 nil,
			CurrentWatermarkTimestamp: 0,
			ProcessTimeCallbackQueue:  &timerQueue[T]{dedupeMap: map[Timer[T]]struct{}{}},
			EventTimeCallbackQueue:    &timerQueue[T]{dedupeMap: map[Timer[T]]struct{}{}},
		}
		ctx.TimerManager().addWatermarkTimerAdvances(name, &service)
		(&service).startAdvanceProcessingTimestamp()
		return service
	}, nil, func(service TimerService[T], err error) (TimerService[T], error) {
		if err != nil {
			return service, err
		}
		service.trigger = trigger
		service.ctx = ctx
		service.nextTimer = nil
		service.EventTimeCallbackQueue.dedupeMap = map[Timer[T]]struct{}{}
		for _, item := range service.EventTimeCallbackQueue.Items {
			service.EventTimeCallbackQueue.dedupeMap[item] = struct{}{}
		}
		service.ProcessTimeCallbackQueue.dedupeMap = map[Timer[T]]struct{}{}
		for _, item := range service.ProcessTimeCallbackQueue.Items {
			service.ProcessTimeCallbackQueue.dedupeMap[item] = struct{}{}
		}
		ctx.TimerManager().addWatermarkTimerAdvances(name, &service)
		(&service).startAdvanceProcessingTimestamp()
		return service, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to init timer service:%w", err)
	} else {
		return timerServiceStateController.Pointer(), nil
	}

}
