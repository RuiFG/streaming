package window

import (
	"streaming/streaming-core/lib/component"
)

type KeySelectorFn[IN any, KEY comparable] func(IN) KEY

type AggregateFn[IN, ACC, OUT any] interface {
	Init(value IN) ACC
	Add(value IN, accumulator ACC) ACC
	GetRestart(accumulator ACC) OUT
	Merge(left, right ACC) ACC
}

type TriggerFn[T any] interface {
	onElement(value T) TriggerResult
}

type AssignerFn[T any] interface {
	assignWindows(value T) []Window
}

type Fn[IN, OUT any, KEY comparable] func(key KEY, window Window, input []IN) []OUT

type RichFn[IN, OUT any, KEY comparable] interface {
	component.Rich
	apply(key KEY, window Window, input []IN) []OUT
}
