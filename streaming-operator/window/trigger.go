package window

import (
	"fmt"
	. "github.com/RuiFG/streaming/streaming-core/operator"
)

type Window struct {
	startTimestamp int64
	endTimestamp   int64
}

func getWindowStartWithOffset(timestamp int64, offset int64, windowSize int64) int64 {
	remainder := (timestamp - offset) % windowSize
	// handle both positive and negative cases
	if remainder < 0 {
		return timestamp - (remainder + windowSize)
	} else {
		return timestamp - remainder
	}

}

func (w Window) Key() string {
	return fmt.Sprintf("%d%d", w.startTimestamp, w.endTimestamp)
}

func (w Window) MaxTimestamp() int64 {
	return w.endTimestamp - 1
}

type TriggerResult int

func (t TriggerResult) IsPurge() bool {
	return t&2 == 1
}

func (t TriggerResult) IsFire() bool {
	return t&1 == 1
}

const (
	Continue     TriggerResult = 0
	Fire         TriggerResult = 1
	Purge        TriggerResult = 2
	FireAndPurge TriggerResult = 3
)

// Trigger

type EventTimeTrigger[KEY comparable, T any] struct {
}

func (e *EventTimeTrigger[KEY, T]) OnElement(ctx WContext[KEY], window Window, key KEY, value T) TriggerResult {
	if window.MaxTimestamp() <= ctx.CurrentEventTimestamp() {
		// if the watermark is already past the window fire immediately
		return Fire
	} else {
		ctx.RegisterEventTimeTimer(Timer[KeyAndWindow[KEY]]{
			Content: KeyAndWindow[KEY]{
				window: window,
				key:    key,
			},
			Timestamp: window.MaxTimestamp(),
		})
		return Continue
	}
}

func (e *EventTimeTrigger[KEY, T]) OnEventTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult {
	if timer.Timestamp == timer.Content.window.MaxTimestamp() {
		return Fire
	} else {
		return Continue
	}
}

func (e *EventTimeTrigger[KEY, T]) OnProcessingTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult {
	return Continue
}

func (e *EventTimeTrigger[KEY, T]) Clear(wContext WContext[KEY], timer Timer[KeyAndWindow[KEY]]) {
	wContext.DeleteEventTimeTimer(timer)
}

func NewEventTimeTrigger[KEY comparable, T any]() TriggerFn[KEY, T] {
	return &EventTimeTrigger[KEY, T]{}
}

type ProcessingTimeTrigger[KEY comparable, T any] struct {
}

func (p *ProcessingTimeTrigger[KEY, T]) OnElement(ctx WContext[KEY], window Window, key KEY, value T) TriggerResult {
	return Continue
}

func (p *ProcessingTimeTrigger[KEY, T]) OnEventTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult {
	return Continue
}

func (p *ProcessingTimeTrigger[KEY, T]) OnProcessingTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult {
	return Fire
}

func (p *ProcessingTimeTrigger[KEY, T]) Clear(wContext WContext[KEY], timer Timer[KeyAndWindow[KEY]]) {
	wContext.DeleteProcessingTimeTimer(timer)
}

func NewProcessingTimeTrigger[KEY comparable, T any]() TriggerFn[KEY, T] {
	return &ProcessingTimeTrigger[KEY, T]{}
}
