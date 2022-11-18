package window

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"strconv"
	"sync"
	"testing"
	"time"
)

type mockCollector[T any] struct {
}

func (c *mockCollector[T]) EmitEvent(event *element.Event[T]) {
	fmt.Println(event.Value)
	//fmt.Println(event.Value)
}

func (c *mockCollector[T]) EmitWatermark(watermark element.Watermark) {
	fmt.Println("current watermark: ", watermark)
}

func (c *mockCollector[T]) EmitWatermarkStatus(_ element.WatermarkStatus) {

}

type aggregator struct {
}

func (a *aggregator) Add(acc string, in string) string {
	if acc == "" {
		return in
	} else {
		return acc + "," + in
	}
}

func (a *aggregator) GetResult(acc string) string {
	return acc
}

func TestAggregateEventTime(t *testing.T) {
	mutex := new(sync.Mutex)
	callerChan := make(chan func())
	go func() {
		for caller := range callerChan {
			mutex.Lock()
			caller()
			mutex.Unlock()
		}
	}()
	ctx := NewContext(log.Global(), store.NewManager("tt", store.NewMemoryBackend()).Controller("test"), callerChan, NewTimerManager())
	operator := &operator[string, string, string, string, string]{
		BaseOperator: BaseOperator[string, any, string]{},
		SelectorFn: func(value string) string {
			atoi, _ := strconv.Atoi(value)
			if atoi%2 == 1 {
				return "1"
			} else {
				return "2"
			}

		},
		TriggerFn:       NewEventTimeTrigger[string, string](),
		AssignerFn:      NewTumblingEventTimeAssigner[string, string](600, 0),
		AggregatorFn:    &aggregator{},
		ProcessWindowFn: &PassThroughProcessWindowFn[string, string]{},
		AllowedLateness: 150,
	}

	operator.Open(ctx, &mockCollector[string]{})
	for i := 0; i < 1201; i++ {
		mutex.Lock()
		if i%20 == 0 {
			operator.ProcessWatermark(element.Watermark(i))
			if i >= 780 {
				time.Sleep(20 * time.Millisecond)
				operator.ProcessEvent(&element.Event[string]{
					Value:        "delay",
					Timestamp:    int64(i) - 200,
					HasTimestamp: true,
				})
			}

		}

		operator.ProcessEvent(&element.Event[string]{
			Value:        strconv.Itoa(i),
			Timestamp:    int64(i),
			HasTimestamp: true,
		})
		mutex.Unlock()
	}

}
