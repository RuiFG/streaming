package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/task"
)

type Stream[T any] interface {
	Env() *Env
	AddDownstream(name string, downstreamInitFn downstreamInitFn[T])
}

// downstreamInitFn is
type downstreamInitFn[T any] func(upstream string) (element.EmitNext[T], []task.Task, error)

type sourceInitFn func() ([]task.Task, []task.Task, error)
