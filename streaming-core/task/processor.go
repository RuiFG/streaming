package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/safe"
	"sync"
)

type CallerAgent func(fn func())

type SynchronousProcessor[IN1, IN2 any] interface {
	ProcessElement1(element.Element[IN1])
	ProcessElement2(element.Element[IN2])
	ProcessCaller(caller func())
}

type channelSynchronousProcess[IN1, IN2 any] struct {
	ctx             _c.Context
	cancel          _c.CancelFunc
	elementHandler  ElementHandler[IN1, IN2]
	element1Channel chan element.Element[IN1]
	element2Channel chan element.Element[IN2]
	callerChannel   chan func()
}

func (c *channelSynchronousProcess[IN1, IN2]) Start() {
	safe.Go(func() error {
		for {
			select {
			case e1 := <-c.element1Channel:
				c.elementHandler.OnElement1(e1)
			case e2 := <-c.element2Channel:
				c.elementHandler.OnElement2(e2)
			case caller := <-c.callerChannel:
				caller()
			case <-c.ctx.Done():
				return nil

			}
		}
	})
}

func (c *channelSynchronousProcess[IN1, IN2]) Stop() {
	c.cancel()
}

func (c *channelSynchronousProcess[IN1, IN2]) ProcessElement1(e element.Element[IN1]) {
	c.element1Channel <- e
}
func (c *channelSynchronousProcess[IN1, IN2]) ProcessElement2(e element.Element[IN2]) {
	c.element2Channel <- e
}

func (c *channelSynchronousProcess[IN1, IN2]) ProcessCaller(caller func()) {
	c.callerChannel <- caller
}

type mutexSyncProcessor[IN1, IN2 any] struct {
	ElementHandler[IN1, IN2]
	mutex *sync.Mutex
}

func (c *mutexSyncProcessor[IN1, IN2]) ProcessElement1(e1 element.Element[IN1]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.OnElement1(e1)
}

func (c *mutexSyncProcessor[IN1, IN2]) ProcessElement2(e2 element.Element[IN2]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.OnElement2(e2)
}

func (c *mutexSyncProcessor[IN1, IN2]) ProcessCaller(caller func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	caller()
}

func newChannelSyncProcessor[IN1, IN2 any](handler ElementHandler[IN1, IN2], channelSize uint) *channelSynchronousProcess[IN1, IN2] {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &channelSynchronousProcess[IN1, IN2]{
		ctx:             ctx,
		cancel:          cancelFunc,
		elementHandler:  handler,
		element1Channel: make(chan element.Element[IN1], channelSize),
		element2Channel: make(chan element.Element[IN2], channelSize),
		callerChannel:   make(chan func(), channelSize),
	}
}

func newMutexSyncProcess[IN1, IN2 any](handler ElementHandler[IN1, IN2]) *mutexSyncProcessor[IN1, IN2] {
	return &mutexSyncProcessor[IN1, IN2]{
		ElementHandler: handler,
		mutex:          new(sync.Mutex),
	}
}
