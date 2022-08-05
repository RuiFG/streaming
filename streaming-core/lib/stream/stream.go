package stream

import (
	"streaming/streaming-core/lib/element"
	"streaming/streaming-core/lib/task"
)

type Stream[T any] interface {
	Env() *Env
	AddDownstream(name string, downstreamInitFn downstreamInitFn[T])
}

// downstreamInitFn is
type downstreamInitFn[T any] func(upstream string) (element.EmitNext[T], []task.Task, error)

type sourceInitFn func() ([]task.Task, []task.Task, error)
