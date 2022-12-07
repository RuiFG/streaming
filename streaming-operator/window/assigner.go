package window

type TumblingEventTimeAssigner[KEY comparable, T any] struct {
	size         int64
	globalOffset int64
}

func (t *TumblingEventTimeAssigner[KEY, T]) AssignWindows(_ WContext[KEY], _ T, eventTimestamp int64) []Window {
	startTimestamp := getWindowStartWithOffset(eventTimestamp, t.globalOffset%t.size, t.size)
	return []Window{{StartTimestamp: startTimestamp, EndTimestamp: startTimestamp + t.size}}
}

func (t *TumblingEventTimeAssigner[KEY, T]) IsEventTime() bool {
	return true
}

type TumblingProcessingTimeAssigner[KEY comparable, T any] struct {
	size         int64
	globalOffset int64
}

func (t *TumblingProcessingTimeAssigner[KEY, T]) AssignWindows(ctx WContext[KEY], _ T, _ int64) []Window {
	startTimestamp := getWindowStartWithOffset(ctx.CurrentProcessingTimestamp(), t.globalOffset%t.size, t.size)
	return []Window{{StartTimestamp: startTimestamp, EndTimestamp: startTimestamp + t.size}}
}

func (t *TumblingProcessingTimeAssigner[KEY, T]) IsEventTime() bool {
	return false
}
