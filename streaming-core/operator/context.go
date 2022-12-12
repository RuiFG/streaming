package operator

import (
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type context struct {
	storeController store.Controller
	logger          log.Logger
	timerManager    *TimerManager
	callerChan      chan *executor.Executor
}

func (c *context) Store() store.Controller {
	return c.storeController
}

func (c *context) Logger() log.Logger {
	return c.logger
}

func (c *context) Exec(fn func()) *executor.Executor {
	newExecutor := executor.NewExecutor(fn)
	c.callerChan <- newExecutor
	return newExecutor
}

func (c *context) TimerManager() *TimerManager {
	return c.timerManager
}

func NewContext(
	logger log.Logger,
	controller store.Controller,
	callerChan chan *executor.Executor,
	manager *TimerManager,
) Context {
	return &context{
		storeController: controller,
		logger:          logger,
		timerManager:    manager,
		callerChan:      callerChan,
	}
}
