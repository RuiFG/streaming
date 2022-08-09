package element

type InputHandler[IN1, IN2 any] interface {
	OnElement1(e1 Element[IN1])
	OnElement2(e2 Element[IN2])
}
type OutputHandler[OUT any] interface {
	OnElement(e Element[OUT])
}

type InputHandlerChain[IN1, IN2 any] []InputHandler[IN1, IN2]

func (handlers InputHandlerChain[IN1, IN2]) OnElement1(e1 Element[IN1]) {
	for _, handler := range handlers {
		handler.OnElement1(e1)
	}
}

func (handlers InputHandlerChain[IN1, IN2]) OnElement2(e2 Element[IN2]) {
	for _, handler := range handlers {
		handler.OnElement2(e2)
	}
}

type OutputHandlerChain[OUT any] []OutputHandler[OUT]

func (handlers OutputHandlerChain[OUT]) OnElement(e Element[OUT]) {
	for _, handler := range handlers {
		handler.OnElement(e)
	}
}
