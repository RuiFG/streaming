package task

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"sync"
)

type CallerAgent func(fn func())

type mutex[IN1, IN2, OUT any] struct {
	inputHandler  element.InputHandler[IN1, IN2]
	outputHandler element.OutputHandler[OUT]
	mutex         *sync.Mutex
}

func (c *mutex[IN1, IN2, OUT]) ProcessInput1(e1 element.Element[IN1]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.inputHandler.OnElement1(e1)
}

func (c *mutex[IN1, IN2, OUT]) ProcessInput2(e2 element.Element[IN2]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.inputHandler.OnElement2(e2)
}
func (c *mutex[IN1, IN2, OUT]) ProcessOutput(e element.Element[OUT]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.outputHandler.OnElement(e)
}

func (c *mutex[IN1, IN2, OUT]) ProcessCaller(caller func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	caller()
}

func newMutexSyncProcess[IN1, IN2, OUT any](inputHandler element.InputHandler[IN1, IN2], outputHandler element.OutputHandler[OUT]) *mutex[IN1, IN2, OUT] {
	return &mutex[IN1, IN2, OUT]{
		inputHandler:  inputHandler,
		outputHandler: outputHandler,
		mutex:         new(sync.Mutex),
	}
}
