package main

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-connector/business-connector/source/geddon"
	mock_sink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"time"
)

var queue []struct{}

func main() {
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder).WithLevel(log.DebugLevel))
	option := stream.DefaultEnvironmentOptions
	option.EnablePeriodicCheckpoint = 5 * time.Second
	env, _ := stream.New(option)
	source, _ := geddon.FromSource[string](env, "geddon",
		geddon.WithDir[string]("./logs", nil, func(left geddon.Location, right geddon.Location) bool {
			return false
		}), geddon.WithFormat[string](func(filename, string2 string) string {
			return string2
		}), geddon.WithPeriodicScan[string](30*time.Second))
	if err := mock_sink.ToSink[string](source, "sink",
		func(in string) {
			fmt.Println(in)
		},
		func(timestamp element.Watermark) {
			fmt.Printf("current watermark timestamp %d\n", timestamp)
		}); err != nil {
		panic(err)
	}
	_ = env.Start()

	time.Sleep(120 * time.Second)
	env.Stop()
}
