package task

import (
	barrier2 "streaming/streaming-core/lib/barrier"
)

type Task interface {
	Name() string
	//Daemon  Running is life cycle
	Daemon() error
	Running() bool

	barrier2.Trigger
	barrier2.Listener
}
