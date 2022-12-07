package watermark

import (
	"math"
	"time"
)

type boundedOutOfOrderlinessWatermarkGeneratorFn[T any] struct {
	maxTimestamp                int64
	outOfOrderlinessMillisecond int64
}

func (b *boundedOutOfOrderlinessWatermarkGeneratorFn[T]) OnEvent(_ T, timestamp int64, _ Collector) {
	b.maxTimestamp = int64(math.Max(float64(b.maxTimestamp), float64(timestamp)))
}

func (b *boundedOutOfOrderlinessWatermarkGeneratorFn[T]) OnPeriodicEmit(watermarkCollector Collector) {
	watermarkCollector.EmitWatermarkTimestamp(b.maxTimestamp - b.outOfOrderlinessMillisecond - 1)
}

type noWatermarksGeneratorFn[T any] struct{}

func (n noWatermarksGeneratorFn[T]) OnEvent(_ T, _ int64, _ Collector) {}

func (n noWatermarksGeneratorFn[T]) OnPeriodicEmit(_ Collector) {}

type IdlenessTimer struct {
	counter                int64
	lastCounter            int64
	startOfInactivityNanos int64
	maxIdleTimeNanos       int64
}

func (i *IdlenessTimer) checkIfIdle() bool {

	if i.counter != i.lastCounter {
		i.lastCounter = i.counter
		i.startOfInactivityNanos = 0
		return false
	} else if i.startOfInactivityNanos == 0 {
		i.startOfInactivityNanos = time.Now().UnixNano()
		return false
	} else {
		return time.Now().UnixNano()-i.startOfInactivityNanos > i.maxIdleTimeNanos
	}
	//if (i.counter != i.lastCounter) {
	//	// activity since the last check. we reset the timer
	//	i.lastCounter = i.counter;
	//	i.startOfInactivityNanos = 0
	//	return false;
	//} else {
	//	// timer started but has not yet reached idle timeout
	//	if (i.startOfInactivityNanos == 0) {
	//		// first time that we see no activity since the last periodic probe
	//		// begin the timer
	//		i.startOfInactivityNanos = time.Now().UnixNano()
	//		return false;
	//	} else {
	//		return clock.relativeTimeNanos()-startOfInactivityNanos > maxIdleTimeNanos;
	//	}
	//}
}

func (i *IdlenessTimer) activity() {
	i.counter++
}

type withIdlenessWatermarkGeneratorFn[T any] struct {
	generator      GeneratorFn[T]
	idleNanosecond int64
	isIdleNow      bool
	idlenessTimer  *IdlenessTimer
}

func (w *withIdlenessWatermarkGeneratorFn[T]) OnEvent(value T, timestamp int64, watermarkCollector Collector) {
	w.generator.OnEvent(value, timestamp, watermarkCollector)
	w.idlenessTimer.activity()
	w.isIdleNow = false
}

func (w *withIdlenessWatermarkGeneratorFn[T]) OnPeriodicEmit(watermarkCollector Collector) {
	if w.idlenessTimer.checkIfIdle() {
		if !w.isIdleNow {
			watermarkCollector.MarkIdle()
			w.isIdleNow = true
		}
	} else {
		w.generator.OnPeriodicEmit(watermarkCollector)
	}
}

func NewWithIdlenessWatermarkGeneratorFn[T any](fn GeneratorFn[T], idleNanosecond int64) GeneratorFn[T] {
	if idleNanosecond < 0 {
		panic("idleNanosecond is negative")
	}
	if fn == nil {
		panic("idleNanosecond is nil")
	}
	return &withIdlenessWatermarkGeneratorFn[T]{
		generator:      fn,
		idleNanosecond: idleNanosecond,
		isIdleNow:      false,
		idlenessTimer: &IdlenessTimer{
			counter:                0,
			lastCounter:            0,
			startOfInactivityNanos: 0,
			maxIdleTimeNanos:       0,
		},
	}
}
