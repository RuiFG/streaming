package main

import (
	"fmt"
	mockSink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-operator/watermark"
	"github.com/RuiFG/streaming/streaming-operator/window"
	"github.com/pkg/profile"
	//kafkaSource "github.com/RuiFG/streaming/streaming-connector/kafka-connector/source"
	mockSource "github.com/RuiFG/streaming/streaming-connector/mock-connector/source"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/stream"
	_map "github.com/RuiFG/streaming/streaming-operator/map"
	"time"
)

type aggregator struct {
}

func (a *aggregator) Add(acc []string, in string) []string {
	return append(acc, in)

}

func (a *aggregator) GetResult(acc []string) int {
	return len(acc)
}

func main() {

	defer profile.Start().Stop()
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder).WithLevel(log.DebugLevel))
	option := stream.DefaultEnvironmentOptions

	env, _ := stream.New(option)
	sourceStream, _ := mockSource.FormSource(env, "mock", func() string {
		return "123"
	}, time.Second, 3000000)
	apply, _ := _map.Apply[string, string](sourceStream, "mm",
		_map.WithFn[string, string](func(string2 string) string {
			return string2
		}))
	aaa, _ := watermark.Apply[string](apply,
		"watermark",
		watermark.WithBoundedOutOfOrderlinessWatermarkGenerator[string](30*time.Second),
		watermark.WithTimestampAssigner[string](func(t string) int64 {
			return time.Now().UnixMilli()
		}))

	agg, _ := window.Apply[struct{}, string, []string, int, int](aaa, "asd",
		window.WithNonKeySelector[string, []string, int, int](),
		window.WithTumblingEventTime[struct{}, string, []string, int, int](60*time.Second, 0),
		window.WithAggregator[struct{}, string, []string, int](&aggregator{}))
	if err := mockSink.ToSink[int](agg, "sink",
		func(in int) {
			fmt.Println(in)
		},
		func(timestamp element.Watermark) {
			fmt.Printf("current watermark timestamp %d\n", timestamp)
		}); err != nil {
		panic(err)
	}
	_ = env.Start()
	time.Sleep(1 * time.Minute)

	_ = env.Stop()
}
