package element

type Type uint

const (
	EventElement Type = iota
	WatermarkElement
	WatermarkStatusElement
)

type Element[T any] interface {
	Type() Type
	AsEvent() *Event[T]
	AsWatermark() *Watermark[T]
	AsWatermarkStatus() *WatermarkStatus[T]
}

type Collector[T any] interface {
	EmitEvent(event *Event[T])
	EmitWatermark(watermark *Watermark[T])
	EmitWatermarkStatus(statusType *WatermarkStatus[T])
}
