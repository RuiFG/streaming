package window

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"

	"testing"
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

	_, err := Apply[string, string, string, string, string](nil, "asd",
		WithAllowedLateness[string, string, string, string, string](123),
		WithAggregator[string, string, string, string](nil))
	if err != nil {
		panic(err)
	}
}
