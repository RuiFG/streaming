package element

type Watermark[T any] struct {
	Timestamp int64
}

func (w *Watermark[T]) Type() Type {
	return WatermarkElement
}

func (w *Watermark[T]) AsEvent() *Event[T] {
	panic("implement me")
}

func (w *Watermark[T]) AsWatermark() *Watermark[T] {
	return w
}

func (w *Watermark[T]) AsWatermarkStatus() *WatermarkStatus[T] {
	//TODO implement me
	panic("implement me")
}

type WatermarkStatusType uint

const (
	IdleWatermarkStatus WatermarkStatusType = iota
	ActiveWatermarkStatus
)

type WatermarkStatus[T any] struct {
	StatusType WatermarkStatusType
}

func (w *WatermarkStatus[T]) Type() Type {
	return WatermarkStatusElement
}

func (w *WatermarkStatus[T]) AsEvent() *Event[T] {
	//TODO implement me
	panic("implement me")
}

func (w *WatermarkStatus[T]) AsWatermark() *Watermark[T] {
	//TODO implement me
	panic("implement me")
}

func (w *WatermarkStatus[T]) AsWatermarkStatus() *WatermarkStatus[T] {
	return w
}
