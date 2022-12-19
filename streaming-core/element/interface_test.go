package element

type NoopCollector[T any] struct{}

func (n *NoopCollector[T]) EmitEvent(_ *Event[T]) {}

func (n *NoopCollector[T]) EmitWatermark(_ Watermark) {}

func (n *NoopCollector[T]) EmitWatermarkStatus(_ WatermarkStatus) {}
