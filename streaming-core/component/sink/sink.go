package sink

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
)

type Default[IN any] struct {
	component.Default
}

func (o *Default[IN]) ProcessEvent(_ *element.Event[IN])                 {}
func (o *Default[IN]) ProcessWatermark(watermark *element.Watermark[IN]) {}
func (o *Default[IN]) NotifyBarrierCome(detail element.Detail)           {}
func (o *Default[IN]) NotifyBarrierComplete(detail element.Detail)       {}
func (o *Default[IN]) NotifyBarrierCancel(detail element.Detail)         {}
