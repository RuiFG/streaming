package source

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/component"
	. "github.com/RuiFG/streaming/streaming-core/component/source"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/Shopify/sarama"
	"sync"
)

type topicAndPartition struct {
	Topic     string
	Partition int32
}

type Config struct {
	SaramaConfig *sarama.Config
	Addresses    []string
	Topics       []string
	GroupId      string
}

type FormatFn[OUT any] func(message *sarama.ConsumerMessage) OUT

type source[OUT any] struct {
	Default[OUT]
	FormatFn[OUT]
	config        Config
	consumerGroup sarama.ConsumerGroup
	state         store.ValueState[map[topicAndPartition]int64]

	offsetMap       *sync.Map
	offsetsToCommit map[int64]map[topicAndPartition]int64
	doneChan        chan struct{}
	commitChan      chan map[topicAndPartition]int64
}

func (s *source[OUT]) Open(ctx component.Context, collector element.Collector[OUT]) (err error) {
	if err = s.Default.Open(ctx, collector); err != nil {
		return err
	}
	s.consumerGroup, err = sarama.NewConsumerGroup(s.config.Addresses, s.config.GroupId, s.config.SaramaConfig)
	if err != nil {
		return err
	}
	return nil
}

func (s *source[OUT]) Run() {
	for {
		var err error
		select {
		case <-s.doneChan:
			return
		default:
			err = s.consumerGroup.Consume(s.Ctx, s.config.Topics, s)
			if err != nil {
				fmt.Println(err)
				//s.ctx.Logger().Warnw("can't consume kafka.", "err", err)
			}

		}
	}

}

func (s *source[OUT]) Close() error {
	close(s.doneChan)
	return nil
}

// ----------------------------------barrier.Listener----------------------------------

func (s *source[OUT]) NotifyBarrierCome(detail element.Detail) {
	m := map[topicAndPartition]int64{}
	s.offsetMap.Range(func(key, value any) bool {
		realKey := key.(topicAndPartition)
		realValue := value.(int64)
		m[realKey] = realValue
		return true
	})
	s.offsetsToCommit[detail.Id] = m
	s.state.Update(m)
}

func (s *source[OUT]) NotifyBarrierComplete(detail element.Detail) {
	if commit, ok := s.offsetsToCommit[detail.Id]; ok {
		s.commitChan <- commit
	}
	delete(s.offsetsToCommit, detail.Id)
}

func (s *source[OUT]) NotifyBarrierCancel(detail element.Detail) {
	delete(s.offsetsToCommit, detail.Id)
}

// ----------------------------------ConsumerGroupHandler----------------------------------

func (s *source[OUT]) Setup(session sarama.ConsumerGroupSession) error {
	s.offsetMap.Range(func(key, value any) bool {
		realKey := key.(topicAndPartition)
		realValue := value.(int64)
		session.ResetOffset(realKey.Topic, realKey.Partition, realValue, "")
		return true
	})

	return nil
}

func (s *source[OUT]) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (s *source[OUT]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			s.Collector.EmitValue(s.FormatFn(message))
			s.offsetMap.Store(topicAndPartition{Topic: message.Topic, Partition: message.Partition}, message.Offset)
		case <-s.doneChan:
			return nil
		case commit := <-s.commitChan:
			for tp, offset := range commit {
				session.MarkOffset(tp.Topic, tp.Partition, offset, "")
			}
			session.Commit()
		}
	}

}

func FromSource[OUT any](env *stream.Env, config Config, formatFn FormatFn[OUT], nameSuffix string) (*stream.SourceStream[OUT], error) {
	return stream.FormSource(env, task.SourceOptions[OUT]{
		Options: task.Options{NameSuffix: nameSuffix},
		New: func() component.Source[OUT] {
			return &source[OUT]{
				FormatFn:        formatFn,
				config:          config,
				offsetMap:       &sync.Map{},
				offsetsToCommit: map[int64]map[topicAndPartition]int64{},
				doneChan:        make(chan struct{}),
				commitChan:      make(chan map[topicAndPartition]int64),
			}
		},
	})

}
