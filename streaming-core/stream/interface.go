package stream

import (
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
)

var (
	ErrMultipleEnv = errors.New("cannot add streams from multiple environments")
)

type Options struct {
	Name        string
	ChannelSize int
}

type Stream[T any] interface {
	Env() *Env
	//Name returns the name of the task to be created.
	Name() string
	addDownstream(name string, downstreamInitFn downstreamInitFn)
	addUpstream(name string)
}

// downstreamInitFn is
type downstreamInitFn func() (task.Emit, []*task.Task, error)

type sourceInitFn func() (*task.Task, []*task.Task, error)
