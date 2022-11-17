package element

type Type uint

type Element[T any] any

type Collector[T any] interface {
	EmitEvent(event *Event[T])
	EmitWatermark(watermark Watermark)
	EmitWatermarkStatus(statusType WatermarkStatus)
}
