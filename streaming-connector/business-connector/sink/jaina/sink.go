package jaina

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/pkg/errors"
	"io/fs"
	"os"
	"path"
	"time"
)

type options[T any] struct {
	dir                     string
	encodeFn                EncodeFn[T]
	enableFlushOnCheckpoint bool
}

type WithOptions[T any] func(o *options[T]) error

func WithEncode[T any](fn EncodeFn[T]) WithOptions[T] {
	return func(o *options[T]) error {
		o.encodeFn = fn
		return nil
	}
}

func WithDir[T any](dir string) WithOptions[T] {
	return func(o *options[T]) error {
		if dir == "" {
			return errors.New("dir can't be nil")
		}
		if stat, err := os.Stat(dir); err != nil {
			return err
		} else {
			if !stat.IsDir() {
				return errors.New(fmt.Sprintf("%s is not a directory", dir))
			}
		}
		o.dir = dir
		return nil
	}
}

func WithFlushOnCheckpoint[T any]() WithOptions[T] {
	return func(o *options[T]) error {
		o.enableFlushOnCheckpoint = true
		return nil
	}
}

type Input struct {
	Filename string
	Buffer   []byte
	FileMode fs.FileMode
}

// EncodeFn encode data to file data
type EncodeFn[T any] func(v T, timestamp int64) *Input

type sink[T any] struct {
	BaseOperator[T, T, any]
	EncodeFn[T]
	dir                     string
	enableFlushOnCheckpoint bool
	state                   []string
}

func (s *sink[T]) Open(ctx Context) error {
	return nil
}

func (s *sink[T]) Close() error {
	return nil
}

func (s *sink[T]) NotifyCheckpointCome(_ int64)     {}
func (s *sink[T]) NotifyCheckpointComplete(_ int64) {}
func (s *sink[T]) NotifyCheckpointCancel(_ int64)   {}

func (s *sink[T]) ProcessEvent(event *element.Event[T]) {
	var encodeTimestamp int64 = 0
	if event.HasTimestamp {
		encodeTimestamp = event.Timestamp
	} else {
		encodeTimestamp = time.Now().UnixMilli()
	}
	data := s.EncodeFn(event.Value, encodeTimestamp)
	if data != nil {
		var filePath string
		if s.enableFlushOnCheckpoint {
			filePath = path.Join(s.dir, ".tmp", data.Filename)
			s.state = append(s.state, data.Filename)
		} else {
			filePath = path.Join(s.dir, data.Filename)
		}
		if err := os.WriteFile(filePath, data.Buffer, data.FileMode); err != nil {
			s.Ctx.Logger().Fatalf("failed to write data to file", "err", err)
		}
	}

}

func ToSink[IN any](upstream stream.Stream[IN], name string, withOptions ...WithOptions[IN]) error {
	o := &options[IN]{
		dir: ".",
	}
	for _, withOptionsFn := range withOptions {
		if err := withOptionsFn(o); err != nil {
			return err
		}
	}

	if upstream.Environment().Options().EnablePeriodicCheckpoint <= 0 {
		return errors.New("jaina sink needs to open periodic checkpoints")
	}
	return stream.ToSink[IN](upstream, stream.SinkStreamOptions[IN]{
		Name: name,
		Sink: &sink[IN]{
			BaseOperator:            BaseOperator[IN, IN, any]{},
			enableFlushOnCheckpoint: o.enableFlushOnCheckpoint,
			EncodeFn:                o.encodeFn,
		},
	})
}
