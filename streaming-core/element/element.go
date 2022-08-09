package element

type Emit[T any] func(element Element[T])

type Type uint

const (
	EventElement Type = iota
	WatermarkElement
	BarrierElement
)

type Element[T any] interface {
	GetMeta() Meta
	Type() Type
	AsEvent() Event[T]
	AsWatermark() Watermark[T]
	AsBarrier() Barrier[T]
}
