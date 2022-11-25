package source

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
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
	BaseOperator[any, any, OUT]
	FormatFn[OUT]
	config        Config
	consumerGroup sarama.ConsumerGroup
	state         *map[topicAndPartition]int64

	offsetMap       *sync.Map
	offsetsToCommit map[int64]map[topicAndPartition]int64
	doneChan        chan struct{}
	commitChan      chan map[topicAndPartition]int64
}

func (s *source[OUT]) Open(ctx Context, collector element.Collector[OUT]) (err error) {
	if err = s.BaseOperator.Open(ctx, collector); err != nil {
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
			s.Collector.EmitEvent(&element.Event[OUT]{
				Value:        s.FormatFn(message),
				Timestamp:    0,
				HasTimestamp: false,
			})
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

func FromSource[OUT any](env *stream.Environment, config Config, formatFn FormatFn[OUT], name string) (*stream.SourceStream[OUT], error) {
	stream.FormSource(env, stream.SourceStreamOptions[OUT]{
		Options: stream.Options{Name: name},
		New: func() Source[OUT] {
			return &source[OUT]{
				FormatFn:        formatFn,
				config:          config,
				offsetMap:       &sync.Map{},
				offsetsToCommit: map[int64]map[topicAndPartition]int64{},
				doneChan:        make(chan struct{}),
				commitChan:      make(chan map[topicAndPartition]int64),
			}
		},
		ElementListeners: nil,
	})

}
