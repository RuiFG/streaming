package element

// Listener listens to each element and can be used for metric records or other services
type Listener[IN1, IN2, OUT any] interface {
	NotifyInput1(e1 Element[IN1])
	NotifyInput2(e2 Element[IN2])
	NotifyOutput(element Element[OUT])
}
