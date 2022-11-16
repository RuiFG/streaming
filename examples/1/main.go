package main

import (
	"fmt"
	mockSink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	"github.com/RuiFG/streaming/streaming-operator/watermark"

	//kafkaSource "github.com/RuiFG/streaming/streaming-connector/kafka-connector/source"
	mockSource "github.com/RuiFG/streaming/streaming-connector/mock-connector/source"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/stream"
	_map "github.com/RuiFG/streaming/streaming-operator/map"
	"github.com/pkg/profile"
	"sync/atomic"
	"time"
)

func main() {
	var counter int64 = 0
	defer profile.Start().Stop()
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder).WithLevel(log.DebugLevel))
	env, _ := stream.New(stream.EnvOptions{})
	//config := sarama.NewConfig()
	//config.Version = sarama.V2_4_0_0
	//config.Consumer.Offsets.AutoCommit.Enable = false
	//config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//sourceStream, _ := kafkaSource.FromSource[string](env, kafkaSource.Config{
	//	SaramaConfig: config,
	//	Addresses:    []string{"10.104.6.165:9092", "10.104.6.167:9092", "10.104.6.168:9092"},
	//	Topics:       []string{"cachelog-huge-c"},
	//	GroupId:      "test-streaming",
	//}, func(message *sarama.ConsumerMessage) string {
	//	return *(*string)(unsafe.Pointer(&message.Value))
	//}, "kafka")
	sourceStream, _ := mockSource.FormSource(env, func() string {
		return "123"
	}, time.Second, 10000000, "mock")
	apply, _ := _map.Apply([]stream.Stream[string]{sourceStream}, func(string2 string) string {
		return string2
	}, "mm")
	aaa, _ := watermark.Apply([]stream.Stream[string]{apply},
		watermark.NewBoundedOutOfOrderlinessWatermarkGeneratorFn[string](0),
		func(string2 string) int64 {
			return time.Now().UnixMilli()
		}, 1*time.Second, "process time")
	if err := mockSink.ToSink([]stream.Stream[string]{aaa},
		func(in string) {
			atomic.AddInt64(&counter, 1)
		},
		func(timestamp int64) {
			fmt.Printf("current watermark timestamp %d\n", timestamp)
		}, "sink"); err != nil {
		panic(err)
	}
	_ = env.Start()
	//env.RegisterBarrierTrigger(func(channel chan<- task.BarrierType) {
	//	for true {
	//		time.Sleep(10 * time.Second)
	//		channel <- task.CheckpointBarrier
	//	}
	//})
	for i := 0; i < 100; i++ {
		fmt.Printf("receive %d\n", counter)
		atomic.AddInt64(&counter, -counter)
		time.Sleep(1 * time.Second)
	}

	_ = env.Stop()
}
