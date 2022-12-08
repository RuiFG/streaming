package main

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-connector/business-connector/source/geddon"
	mock_sink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"strings"
	"time"
)

func main() {
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder).WithLevel(log.DebugLevel))
	option := stream.DefaultEnvironmentOptions
	option.EnablePeriodicCheckpoint = 5 * time.Second
	env, _ := stream.New(option)
	source, err := geddon.FromSource[string](env, "geddon",
		geddon.WithDir[string]("/Users/klein/GoLandProjects/streaming/examples/2/logs", nil), geddon.WithFormat[string](func(filename string, data []byte) string {
			return strings.TrimSpace(string(data))
		}, '\n'), geddon.WithPeriodicScan[string](30*time.Second))
	if err != nil {
		panic(err)
	}
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
	env.Stop(false)
}
