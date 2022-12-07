package window

import (
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type KeyAndWindow[KEY comparable] struct {
	Key    KEY
	Window Window
}

// SelectorFn will select the Key from the event
type SelectorFn[KEY comparable, IN any] func(IN) KEY

type WContext[KEY comparable] interface {
	//TimeService functions
	CurrentProcessingTimestamp() int64
	CurrentEventTimestamp() int64
	RegisterEventTimeTimer(timer Timer[KeyAndWindow[KEY]])
	RegisterProcessingTimeTimer(timer Timer[KeyAndWindow[KEY]])
	DeleteEventTimeTimer(timer Timer[KeyAndWindow[KEY]])
	DeleteProcessingTimeTimer(timer Timer[KeyAndWindow[KEY]])

	//sotre.Contrller functions
	store.Controller
}

type TriggerFn[KEY comparable, T any] interface {
	OnElement(ctx WContext[KEY], window Window, key KEY, value T) TriggerResult
	OnEventTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult
	OnProcessingTimer(timer Timer[KeyAndWindow[KEY]]) TriggerResult
	Clear(wContext WContext[KEY], timer Timer[KeyAndWindow[KEY]])
}

type AssignerFn[KEY comparable, T any] interface {
	AssignWindows(ctx WContext[KEY], value T, eventTimestamp int64) []Window
	IsEventTime() bool
}

type AggregatorFn[IN, ACC, OUT any] interface {
	Add(ACC, IN) ACC
	GetResult(ACC) OUT
}

type ProcessWindowFn[KEY comparable, IN, OUT any] interface {
	Process(window Window, key KEY, input IN) []OUT
	Clear(Window, KEY)
}

type RichFn[IN, OUT any, KEY comparable] interface {
	Rich
	apply(key KEY, window Window, input []IN) []OUT
}

// PassThroughProcessWindowFn is ProcessWindowFn implementation,
// which is used to adapt to the case of only aggregation or reduction
type PassThroughProcessWindowFn[KEY comparable, T any] struct{}

func (p *PassThroughProcessWindowFn[KEY, T]) Process(_ Window, _ KEY, input T) []T { return []T{input} }

func (p *PassThroughProcessWindowFn[KEY, T]) Clear(_ Window, _ KEY) {}
