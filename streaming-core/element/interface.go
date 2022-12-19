package element

type Element any

type Emit func(element Element)

type Collector[T any] interface {
	EmitEvent(event *Event[T])
	EmitWatermark(watermark Watermark)
	EmitWatermarkStatus(statusType WatermarkStatus)
}
