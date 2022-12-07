package element

type (
	Watermark       int64
	WatermarkStatus uint
)

const (
	IdleWatermarkStatus WatermarkStatus = iota
	ActiveWatermarkStatus
)
