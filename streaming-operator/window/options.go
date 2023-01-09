package window

import (
	"errors"
	"github.com/RuiFG/streaming/streaming-core/store"
	"time"
)

type options[KEY comparable, IN, ACC, WIN, OUT any] struct {
	selectorFn      SelectorFn[KEY, IN]
	triggerFn       TriggerFn[KEY, IN]
	assignerFn      AssignerFn[KEY, IN]
	processWindowFn ProcessWindowFn[KEY, WIN, OUT]
	aggregatorFn    AggregatorFn[IN, ACC, WIN]
	stateDescriptor store.StateDescriptor[map[Window]map[KEY]ACC]
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
			return errors.New("SelectFn can't be nil")
		}
		opts.selectorFn = fn
		return nil
	}
}

func WithTumblingEventTime[KEY comparable, IN, ACC, WIN, OUT any](windowSize time.Duration, globalOffset time.Duration) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if windowSize < time.Millisecond {
			return errors.New("windowSize should be greater than milliseconds")
		}
		if globalOffset < time.Millisecond && globalOffset != 0 {
			return errors.New("globalOffset should be greater than milliseconds or equal to 0")
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
			return errors.New("windowSize should be greater than milliseconds")
		}
		if globalOffset < time.Millisecond && globalOffset != 0 {
			return errors.New("globalOffset should be greater than milliseconds or equal to 0")
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
			return errors.New("allowedLateness can't less than 0")
		}
		opts.allowedLateness = allowedLateness
		return nil
	}
}

func WithTrigger[KEY comparable, IN, ACC, WIN, OUT any](fn TriggerFn[KEY, IN]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.New("TriggerFn can't ne nil")
		}
		opts.triggerFn = fn
		return nil
	}
}

func WithAggregator[KEY comparable, IN, ACC, WIN, OUT any](fn AggregatorFn[IN, ACC, WIN]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.New("AggregatorFn can't ne nil")
		}
		opts.aggregatorFn = fn
		return nil
	}
}

func WithProcess[KEY comparable, IN, ACC, WIN, OUT any](fn ProcessWindowFn[KEY, WIN, OUT]) WithOptions[KEY, IN, ACC, WIN, OUT] {
	return func(opts *options[KEY, IN, ACC, WIN, OUT]) error {
		if fn == nil {
			return errors.New("ProcessWindowFn can't ne nil")
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

func WithStateDescriptor[KEY comparable, IN, ACC, WIN, OUT any](stateDescriptor store.StateDescriptor[map[Window]map[KEY]ACC]) WithOptions[KEY, IN, ACC, OUT, OUT] {
	return func(opts *options[KEY, IN, ACC, OUT, OUT]) error {
		if stateDescriptor.Key == "" || stateDescriptor.Serializer == nil ||
			stateDescriptor.Deserializer == nil || stateDescriptor.Initializer == nil {
			return errors.New("stateDescriptor value can't be nil")
		}
		opts.stateDescriptor = stateDescriptor
		return nil
	}
}
