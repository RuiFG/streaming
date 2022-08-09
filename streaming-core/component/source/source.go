package source

import (
	"github.com/RuiFG/streaming/streaming-core/component"
	"github.com/RuiFG/streaming/streaming-core/element"
)

type Default[OUT any] struct {
	component.Default
	Collector element.Collector[OUT]
}

func (o *Default[OUT]) Open(ctx component.Context, collector element.Collector[OUT]) error {
	if err := o.Default.Open(ctx); err != nil {
		return err
	}
	o.Collector = collector
	return nil
}

func (o *Default[OUT]) NotifyBarrierCome(detail element.Detail) {}

func (o *Default[OUT]) NotifyBarrierComplete(detail element.Detail) {}

func (o *Default[OUT]) NotifyBarrierCancel(detail element.Detail) {}
