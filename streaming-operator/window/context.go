package window

import (
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
)

type context[KEY comparable] struct {
	store.Controller
	*TimerService[KeyAndWindow[KEY]]
}

func (c *context[KEY]) RegisterEventTimeTimer(timer Timer[KeyAndWindow[KEY]]) {
	c.TimerService.RegisterEventTimeTimer(timer)
}

func (c *context[KEY]) RegisterProcessingTimeTimer(timer Timer[KeyAndWindow[KEY]]) {
	c.TimerService.RegisterProcessingTimeTimer(timer)
}

func (c *context[KEY]) DeleteEventTimeTimer(timer Timer[KeyAndWindow[KEY]]) {
	c.TimerService.DeleteEventTimeTimer(timer)
}

func (c *context[KEY]) DeleteProcessingTimeTimer(timer Timer[KeyAndWindow[KEY]]) {
	c.TimerService.DeleteProcessingTimeTimer(timer)
}
