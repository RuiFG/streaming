package operator

import (
	"github.com/RuiFG/streaming/streaming-core/common/executor"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
)

type context struct {
	storeController store.Controller
	logger          *zap.Logger
	timerManager    *TimerManager
	callerChan      chan *executor.Executor
	scope           tally.Scope
}

func (c *context) Store() store.Controller {
	return c.storeController
}

func (c *context) Logger() *zap.Logger {
	return c.logger
}

func (c *context) Scope() tally.Scope {
	return c.scope
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
	logger *zap.Logger,
	scope tally.Scope,
	controller store.Controller,
	callerChan chan *executor.Executor,
	manager *TimerManager,
) Context {
	return &context{
		storeController: controller,
		logger:          logger,
		scope:           scope,
		timerManager:    manager,
		callerChan:      callerChan,
	}
}
