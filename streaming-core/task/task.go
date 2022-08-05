package task

import (
	"github.com/RuiFG/streaming/streaming-core/barrier"
)

type Task interface {
	Name() string
	//Daemon  Running is life cycle
	Daemon() error
	Running() bool

	barrier.Trigger
	barrier.Listener
}
