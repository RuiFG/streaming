package operator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTimerQueue_Peek(t *testing.T) {
	qu := &timerQueue[string]{dedupeMap: map[Timer[string]]struct{}{}}
	qu.PushTimer(Timer[string]{
		Content:   "tt",
		Timestamp: 2,
	})
	qu.PushTimer(Timer[string]{
		Content:   "t",
		Timestamp: 1,
	})
	qu.PushTimer(Timer[string]{
		Content:   "ttt",
		Timestamp: 3,
	})
	peek := qu.PeekTimer()
	assert.Equal(t, "t", peek.Content)
	assert.Equal(t, int64(1), peek.Timestamp)
	assert.Equal(t, 3, qu.Len())
}

func TestTimerInternalHeap_Pop(t *testing.T) {
	qu := &timerQueue[string]{dedupeMap: map[Timer[string]]struct{}{}}
	qu.PushTimer(Timer[string]{
		Content:   "tt",
		Timestamp: 2,
	})
	qu.PushTimer(Timer[string]{
		Content:   "t",
		Timestamp: 1,
	})
	qu.PushTimer(Timer[string]{
		Content:   "ttt",
		Timestamp: 3,
	})
	assert.Equal(t, "t", qu.PopTimer().Content)
	assert.Equal(t, 2, qu.Len())
	assert.Equal(t, "tt", qu.PopTimer().Content)
	assert.Equal(t, "ttt", qu.PopTimer().Content)
	pop := qu.PopTimer()
	assert.Equal(t, pop, Timer[string]{})
}

func TestTimerQueue_Remove(t *testing.T) {
	qu := &timerQueue[string]{dedupeMap: map[Timer[string]]struct{}{}}
	qu.PushTimer(Timer[string]{
		Content:   "tt",
		Timestamp: 2,
	})
	qu.PushTimer(Timer[string]{
		Content:   "t",
		Timestamp: 1,
	})
	qu.PushTimer(Timer[string]{
		Content:   "ttt",
		Timestamp: 3,
	})

	assert.True(t, qu.Remove(Timer[string]{
		Content:   "t",
		Timestamp: 1,
	}))
}
