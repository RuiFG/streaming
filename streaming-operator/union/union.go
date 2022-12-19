package union

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
)

type options[IN any] struct {
	rich Rich
}

type WithOptions[IN any] func(options *options[IN]) error

func WithRich[IN any](rich Rich) WithOptions[IN] {
	return func(opts *options[IN]) error {
		opts.rich = rich
		return nil
	}
}

type operator[T any] struct {
	rich             Rich
	emit             element.Emit
	combineWatermark *CombineWatermark
	inputCount       int
}

func (o *operator[T]) NotifyCheckpointCome(checkpointId int64) {

}

func (o *operator[T]) NotifyCheckpointComplete(checkpointId int64) {

}

func (o *operator[T]) NotifyCheckpointCancel(checkpointId int64) {

}

func (o *operator[T]) Open(ctx Context, emit element.Emit) error {
	if err := o.rich.Open(ctx); err != nil {
		return err
	}
	o.combineWatermark = NewCombineWatermark(o.inputCount)
	return nil
}

func (o *operator[T]) Close() error {
	return o.rich.Close()
}

func (o *operator[T]) ProcessElement(e element.Element, index int) {
	switch value := e.(type) {
	case *element.Event[T]:
		o.emit(value)
	case element.Watermark:
		o.processWatermark(value, index)
	case element.WatermarkStatus:
		o.processWatermarkStatus(value, index)
	}
}

func (o *operator[T]) processWatermark(watermark element.Watermark, input int) {
	if o.combineWatermark.UpdateWatermark(watermark, input) {
		o.emit(o.combineWatermark.GetCombinedWatermark())
	}
}

func (o *operator[T]) processWatermarkStatus(watermarkStatus element.WatermarkStatus, input int) {
	wasIdle := o.combineWatermark.IsIdle()
	if o.combineWatermark.UpdateIdle(watermarkStatus == element.IdleWatermarkStatus, input) {
		o.emit(o.combineWatermark.GetCombinedWatermark())
	}
	if wasIdle != o.combineWatermark.IsIdle() {
		o.emit(watermarkStatus)
	}
}

func Apply[IN any](upstreams []stream.Stream[IN], name string, withOptions ...WithOptions[IN]) (stream.Stream[IN], error) {
	o := &options[IN]{}
	for _, withOptionsFn := range withOptions {
		if err := withOptionsFn(o); err != nil {
			return nil, err
		}
	}

	return stream.ApplyMultiInput[IN](upstreams, stream.OperatorStreamOptions{
		Name: name,
		Operator: &operator[IN]{
			rich:       o.rich,
			inputCount: len(upstreams),
		},
	})
}
