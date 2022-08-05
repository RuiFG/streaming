package component

import (
	_c "context"
	"streaming/streaming-core/lib/log"
	"streaming/streaming-core/lib/service"
	"streaming/streaming-core/lib/store"
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
