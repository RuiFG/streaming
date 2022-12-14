package stream

import (
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
)

var (
	ErrMultipleEnv = errors.New("cannot add streams from multiple environments")
)

type Stream[T any] interface {
	Generics() T
	Environment() *Environment
	//Name returns the LogStream of the task to be created.
	Name() string
	addDownstream(name string, downstreamInitFn downstreamInitFn)
	addUpstream(name string)
}

type downstreamInitFn func() (task.Emit, []*task.Task, error)

type sourceInitFn func() (*task.Task, []*task.Task, error)
