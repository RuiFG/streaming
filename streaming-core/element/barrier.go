package element

import (
	"github.com/RuiFG/streaming/streaming-core/barrier"
)

type Barrier[_ any] struct {
	Meta
	barrier.Detail
}

func (b *Barrier[_]) GetMeta() Meta {
	return b.Meta
}

func (b *Barrier[_]) Type() Type {
	return BarrierElement
}

func (b *Barrier[_]) AsEvent() *Event[_] {
	panic("implement me")
}

func (b *Barrier[_]) AsWatermark() *Watermark[_] {

	panic("implement me")
}

func (b *Barrier[_]) AsBarrier() *Barrier[_] {
	return b
}
