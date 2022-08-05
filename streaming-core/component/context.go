package component

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type context struct {
	_c.Context
}

func (c *context) TimeScheduler() service.TimeScheduler {
	//TODO implement me
	panic("implement me")
}

func (c *context) Store() store.Manager {
	//TODO implement me
	panic("implement me")
}

func (c *context) Logger() log.Logger {
	return nil
}

func NewContext(ctx _c.Context, name string) Context {
	return &context{ctx}
}
