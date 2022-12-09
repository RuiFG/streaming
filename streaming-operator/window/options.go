package window

import (
	"github.com/pkg/errors"
	"time"
)

type options[KEY comparable, IN, ACC, WIN, OUT any] struct {
	selectorFn      SelectorFn[KEY, IN]
	triggerFn       TriggerFn[KEY, IN]
	assignerFn      AssignerFn[KEY, IN]
	processWindowFn ProcessWindowFn[KEY, WIN, OUT]
	aggregatorFn    AggregatorFn[IN, ACC, WIN]
	allowedLateness int64
}

type WithOptions[KEY comparable, IN, ACC, WIN, OUT any] func(opts *options[KEY, IN, ACC, WIN, OUT]) error

func WithNonKeySelector[IN, ACC, WIN, OUT any]() WithOptions[struct{}, IN, ACC, WIN, OUT] {
	return func(opts *options[struct{}, IN, ACC, WIN, OUT]) error {
		nonKey := struct{}{}
		opts.selectorFn = func(IN) struct{} {
			return nonKey
		}
		return nil
	}
}

func WithKeySelector[KEY comparable, IN, ACC, WIN, OUT any](fn SelectorFn[KEY, IN]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.Errorf("SelectFn can't be nil")
		}
		opts.selectorFn = fn
		return nil
	}
}

func WithTumblingEventTime[KEY comparable, IN, ACC, WIN, OUT any](windowSize time.Duration, globalOffset time.Duration) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if windowSize < time.Millisecond {
			return errors.Errorf("windowSize should be greater than milliseconds")
		}
		if globalOffset < time.Millisecond && globalOffset != 0 {
			return errors.Errorf("globalOffset should be greater than milliseconds or equal to 0")
		}
		opts.assignerFn = &TumblingEventTimeAssigner[KEY, IN]{
			size:         int64(windowSize / time.Millisecond),
			globalOffset: int64(globalOffset / time.Millisecond),
		}
		opts.triggerFn = &EventTimeTrigger[KEY, IN]{}
		return nil
	}
}

func WithTumblingProcessingTime[KEY comparable, IN, ACC, WIN, OUT any](windowSize time.Duration, globalOffset time.Duration) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if windowSize < time.Millisecond {
			return errors.Errorf("windowSize should be greater than milliseconds")
		}
		if globalOffset < time.Millisecond && globalOffset != 0 {
			return errors.Errorf("globalOffset should be greater than milliseconds or equal to 0")
		}
		opts.assignerFn = &TumblingProcessingTimeAssigner[KEY, IN]{
			size:         int64(windowSize / time.Millisecond),
			globalOffset: int64(globalOffset / time.Millisecond),
		}
		opts.triggerFn = &ProcessingTimeTrigger[KEY, IN]{}
		return nil
	}
}

func WithAllowedLateness[KEY comparable, IN, ACC, WIN, OUT any](allowedLateness int64) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if allowedLateness < 0 {
			return errors.Errorf("allowedLateness can't less than 0")
		}
		return nil
	}
}

func WithTrigger[KEY comparable, IN, ACC, WIN, OUT any](fn TriggerFn[KEY, IN]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.Errorf("TriggerFn can't ne nil")
		}
		opts.triggerFn = fn
		return nil
	}
}

func WithAggregator[KEY comparable, IN, ACC, WIN, OUT any](fn AggregatorFn[IN, ACC, WIN]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.Errorf("AggregatorFn can't ne nil")
		}
		opts.aggregatorFn = fn
		return nil
	}
}

func WithProcess[KEY comparable, IN, ACC, WIN, OUT any](fn ProcessWindowFn[KEY, WIN, OUT]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.Errorf("ProcessWindowFn can't ne nil")
		}
		opts.processWindowFn = fn
		return nil
	}
}

func WithPassThroughProcess[KEY comparable, IN, ACC, OUT any]() WithOptions[KEY, IN, ACC, OUT, OUT] {
	return func(opts *options[KEY, IN, ACC, OUT, OUT]) error {
		opts.processWindowFn = &PassThroughProcessWindowFn[KEY, OUT]{}
		return nil
	}

}
