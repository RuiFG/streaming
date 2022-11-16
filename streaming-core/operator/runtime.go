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

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermark1(watermark *element.Watermark[IN1]) {
	r.processWatermarkTimestamp(watermark.Timestamp, 1)
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermark2(watermark *element.Watermark[IN2]) {
	r.processWatermarkTimestamp(watermark.Timestamp, 2)
}

func (r *Runtime[IN1, IN2, OUT]) processWatermarkTimestamp(watermarkTimestamp int64, input int) {
	if r.combineWatermark.UpdateWatermarkTimestamp(watermarkTimestamp, input) {
		r.Operator.ProcessWatermarkTimestamp(r.combineWatermark.GetCombinedWatermarkTimestamp())
	}
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermarkStatus1(watermarkStatus *element.WatermarkStatus[IN1]) {
	r.processWatermarkStatusType(watermarkStatus.StatusType, 1)
}

func (r *Runtime[IN1, IN2, OUT]) ProcessWatermarkStatus2(watermarkStatus *element.WatermarkStatus[IN2]) {
	r.processWatermarkStatusType(watermarkStatus.StatusType, 2)
}

func (r *Runtime[IN1, IN2, OUT]) processWatermarkStatusType(watermarkStatusType element.WatermarkStatusType, input int) {
	wasIdle := r.combineWatermark.IsIdle()
	if r.combineWatermark.UpdateIdle(watermarkStatusType == element.IdleWatermarkStatus, input) {
		r.ProcessWatermarkTimestamp(r.combineWatermark.GetCombinedWatermarkTimestamp())
	}
	if wasIdle != r.combineWatermark.IsIdle() {
		r.Operator.ProcessWatermarkStatus(watermarkStatusType)
	}
}
