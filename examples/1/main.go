package main

import (
	kafkaSource "github.com/RuiFG/streaming/streaming-connector/kafka-connector/source"
	mockSink "github.com/RuiFG/streaming/streaming-core/component/sink/mock"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/Shopify/sarama"
	"github.com/pkg/profile"
	"sync/atomic"
	"time"
)

func main() {
	var counter int64 = 0
	defer profile.Start().Stop()
	env := stream.New(stream.Options{})
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	sourceStream, _ := kafkaSource.FromSource[string](env, kafkaSource.Config{
		SaramaConfig: config,
		Addresses:    []string{"10.104.6.165:9092", "10.104.6.167:9092", "10.104.6.168:9092"},
		Topics:       []string{"cachelog-huge-c"},
		GroupId:      "test-streaming",
	}, func(message *sarama.ConsumerMessage) string {
		return string(message.Value)
	}, "kafka")

	if err := mockSink.ToSink([]stream.Stream[string]{sourceStream},
		func(in string) {
			atomic.AddInt64(&counter, 1)
		},
		func(time time.Time) {}, "sink"); err != nil {
		panic(err)
	}
	_ = env.Start()

	time.Sleep(10 * time.Second)

	_ = env.Stop()
}
