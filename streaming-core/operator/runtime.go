package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"sync"
)

// Runtime is the operator runtime,
// including functions to process element.Watermark and element.WatermarkStatus
type Runtime[IN1, IN2, OUT any] struct {
	Operator[IN1, IN2, OUT]
	collector             element.Collector[OUT]
	combineWatermarkMutex *sync.RWMutex
	combineWatermark      *CombineWatermark
}

func (r *Runtime[IN1, IN2, OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	combineWatermark, mutex, err := store.RegisterOrGet(ctx.Store(), NewCombineWatermarkStateDescriptor("combine-watermark", 2))
	if err != nil {
		return errors.WithMessage(err, "failed to register combine watermark.")
	}
	r.combineWatermark = combineWatermark
	r.combineWatermarkMutex = mutex
	return r.Operator.Open(ctx, collector)
}

func (r *Runtime[IN1, IN2, OUT]) Close() error {
	return r.Operator.Close()
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermark1(watermark element.Watermark) {
	r.processWatermark(watermark, 1)
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermark2(watermark element.Watermark) {
	r.processWatermark(watermark, 2)
}

func (r *Runtime[IN1, IN2, OUT]) processWatermark(watermark element.Watermark, input int) {
	if r.combineWatermark.UpdateWatermarkTimestamp(int64(watermark), input) {
		r.Operator.ProcessWatermark(element.Watermark(r.combineWatermark.GetCombinedWatermarkTimestamp()))
	}
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermarkStatus1(watermarkStatus element.WatermarkStatus) {
	r.processWatermarkStatus(watermarkStatus, 1)
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermarkStatus2(watermarkStatus element.WatermarkStatus) {
	r.processWatermarkStatus(watermarkStatus, 2)
}

func (r *Runtime[IN1, IN2, OUT]) processWatermarkStatus(watermarkStatus element.WatermarkStatus, input int) {
	wasIdle := r.combineWatermark.IsIdle()
	if r.combineWatermark.UpdateIdle(watermarkStatus == element.IdleWatermarkStatus, input) {
		r.ProcessWatermark(element.Watermark(r.combineWatermark.GetCombinedWatermarkTimestamp()))
	}
	if wasIdle != r.combineWatermark.IsIdle() {
		r.Operator.ProcessWatermarkStatus(watermarkStatus)
	}
}
