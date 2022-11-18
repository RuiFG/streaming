package window

type TumblingEventTimeAssigner[KEY comparable, T any] struct {
	size         int64
	globalOffset int64
}

func (t *TumblingEventTimeAssigner[KEY, T]) AssignWindows(_ WContext[KEY], _ T, eventTimestamp int64) []Window {
	startTimestamp := getWindowStartWithOffset(eventTimestamp, t.globalOffset%t.size, t.size)
	return []Window{{startTimestamp: startTimestamp, endTimestamp: startTimestamp + t.size}}
}

func (t *TumblingEventTimeAssigner[KEY, T]) IsEventTime() bool {
	return true
}

func NewTumblingEventTimeAssigner[KEY comparable, T any](size int64, offset int64) AssignerFn[KEY, T] {
	return &TumblingEventTimeAssigner[KEY, T]{
		size:         size,
		globalOffset: offset,
	}
}

type TumblingProcessingTimeAssigner[KEY comparable, T any] struct {
	size         int64
	globalOffset int64
}

func (t *TumblingProcessingTimeAssigner[KEY, T]) AssignWindows(ctx WContext[KEY], _ T, _ int64) []Window {
	startTimestamp := getWindowStartWithOffset(ctx.CurrentProcessingTimestamp(), t.globalOffset%t.size, t.size)
	return []Window{{startTimestamp: startTimestamp, endTimestamp: startTimestamp + t.size}}
}

func (t *TumblingProcessingTimeAssigner[KEY, T]) IsEventTime() bool {
	return false
}

func NewTumblingProcessingTimeAssigner[KEY comparable, T any](size int64, offset int64) AssignerFn[KEY, T] {
	return &TumblingProcessingTimeAssigner[KEY, T]{
		size:         size,
		globalOffset: offset,
	}
}
