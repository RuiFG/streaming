package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
)

var (
	ErrMultipleEnv = errors.New("cannot add streams from multiple environments")
)

type Stream[T any] interface {
	Env() *Env
	Name() string
	addDownstream(name string, downstreamInitFn downstreamInitFn[T])
	addUpstream(name string)
}

// downstreamInitFn is
type downstreamInitFn[T any] func(upstream string) (element.Emit[T], []task.Task, error)

type sourceInitFn func() (task.Task, []task.Task, error)
