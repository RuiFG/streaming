package task

import (
	"github.com/RuiFG/streaming/streaming-core/service"
	"time"
)

type callbackAgent struct {
	cb    service.TimeCallback
	agent CallerAgent
}

func (c *callbackAgent) OnProcessingTime(processingTime time.Time) {
	c.agent(func() {
		c.cb.OnProcessingTime(processingTime)
	})
}
