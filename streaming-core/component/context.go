package component

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/service"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type context struct {
	_c.Context
	storeController store.Controller
	logger          log.Logger
	timeScheduler   service.TimeScheduler
}

func (c *context) TimeScheduler() service.TimeScheduler {
	return c.timeScheduler
}

func (c *context) Store() store.Controller {
	return c.storeController
}

func (c *context) Logger() log.Logger {
	return c.logger
}

func NewContext(ctx _c.Context,
	controller store.Controller,
	scheduler service.TimeScheduler,
	logger log.Logger) Context {
	return &context{
		Context:         ctx,
		storeController: controller,
		logger:          logger,
		timeScheduler:   scheduler,
	}
}
