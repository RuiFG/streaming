package geddon

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/pkg/errors"
	"os"
	"regexp"
	"time"
)

type options[T any] struct {
	formatFn     FormatFn[T]
	delim        byte
	timestampFn  TimestampFn
	comparatorFn ComparatorFn
	dir          string
	pattern      *regexp.Regexp

	periodicScan time.Duration
}

type WithOptions[T any] func(options *options[T]) error

func WithFormat[T any](fn FormatFn[T], delim byte) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.formatFn = fn
		opts.delim = delim
		return nil
	}
}

func WithTimestamp[T any](fn TimestampFn) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.timestampFn = fn
		return nil
	}
}

func WithDir[T any](dir string, pattern *regexp.Regexp) WithOptions[T] {
	return func(opts *options[T]) error {
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
		if pattern == nil {
			pattern = regexp.MustCompile(".*")
		}
		opts.dir = dir
		opts.pattern = pattern
		return nil
	}
}

func WithPeriodicScan[T any](duration time.Duration) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.periodicScan = duration
		return nil
	}
}

func WithComparator[T any](fn ComparatorFn) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.comparatorFn = fn
		return nil
	}
}

type FormatFn[OUT any] func(filename string, line []byte) OUT

type TimestampFn func(filename string, line []byte) int64

type ComparatorFn func(left Location, right Location) bool

type State struct {
	//the file location of currently being read
	//not currently read if it's emptyLocation.
	ReadingLocation Location

	PendingCheckpoints []int64
	//map[checkpointId][]ReadiedFilenames
	PendingRemove map[int64][]Location
}

type source[OUT any] struct {
	BaseOperator[any, any, OUT]
	FormatFn[OUT]
	delim byte
	ComparatorFn
	TimestampFn
	//dir is a log directory that needs to be watch and scan,
	//whenever a file is created in the directory, it will be read and emit to the downstream by line.
	dir string
	//it is processed only if the log file name matches the pattern.
	pattern *regexp.Regexp

	//periodic scan log dir path, if set 0, will not be enabled
	enablePeriodicScan time.Duration

	done chan struct{}

	state            *State
	readiedLocations []Location
	reader           *Reader[OUT]
	enumerator       *Enumerator
}

func (s *source[OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	if err := s.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	s.done = make(chan struct{})
	s.reader = NewReader(ctx, s.done, collector, s.FormatFn, s.delim, s.TimestampFn)
	s.enumerator = NewEnumerator(ctx, s.done, s.dir, s.pattern, s.enablePeriodicScan, s.ComparatorFn)
	if fileStateController, err := store.GobRegisterOrGet[State](s.Ctx.Store(), "state",
		func() State {
			return State{
				ReadingLocation:    emptyLocation,
				PendingCheckpoints: nil,
				PendingRemove:      map[int64][]Location{},
			}
		}, nil, nil); err != nil {
		return errors.WithMessage(err, "failed to register or get geddon state")
	} else {
		s.state = fileStateController.Pointer()
	}
	//restore the last checkpoint state
	if len(s.state.PendingCheckpoints) > 0 {
		lastCheckpointId := s.state.PendingCheckpoints[len(s.state.PendingCheckpoints)-1]
		recoverFileLocations := s.state.PendingRemove[lastCheckpointId]
		s.state.PendingCheckpoints = s.state.PendingCheckpoints[0 : len(s.state.PendingCheckpoints)-1]
		delete(s.state.PendingRemove, lastCheckpointId)
		for _, location := range recoverFileLocations {
			s.enumerator.AddLocation(location)
		}
		for _, locations := range s.state.PendingRemove {
			s.enumerator.AddDedupeLocations(locations)
		}
	}
	if s.state.ReadingLocation != emptyLocation {
		s.enumerator.AddLocation(s.state.ReadingLocation)
	}
	//scan the directory for the first time startup
	s.enumerator.ScanDir()
	if err := s.enumerator.StartWatchDir(); err != nil {
		return errors.WithMessage(err, "failed to start watch dir")
	}
	if s.enablePeriodicScan > 0 {
		s.enumerator.StartPeriodicScanDir()
	}
	return nil
}

func (s *source[OUT]) Close() error {
	close(s.done)
	return nil
}

func (s *source[OUT]) Run() {
	for {
		select {
		case <-s.done:
			return
		default:
			if location, ok := s.enumerator.Next(); ok {
				if err := s.reader.Read(location); err != nil {
					s.Ctx.Logger().Warnw("can't read file location.", "location", location)
				} else {
					s.readiedLocations = append(s.readiedLocations, location)
				}
			} else {
				continue
			}

		}
	}
}

func (s *source[OUT]) NotifyCheckpointCome(checkpointId int64) {
	//1. truncate the current offset
	//2. add checkpoints to the pending list
	//3. save the list of files that have been read at the current checkpoint
	//4. clear the readied locations list
	s.state.ReadingLocation = s.reader.SnapshotState()
	s.state.PendingCheckpoints = append(s.state.PendingCheckpoints, checkpointId)
	s.state.PendingRemove[checkpointId] = s.readiedLocations
	s.readiedLocations = nil
}

func (s *source[OUT]) NotifyCheckpointComplete(checkpointId int64) {
	for i := 0; i < len(s.state.PendingCheckpoints); i++ {
		pendingCheckpointId := s.state.PendingCheckpoints[0]
		if pendingCheckpointId < checkpointId {
			for _, location := range s.state.PendingRemove[pendingCheckpointId] {
				if err := os.Remove(location.AbsolutePath); err != nil {
					s.Ctx.Logger().Warnw(fmt.Sprintf("%s file from the last checkpoint cannot be deleted, maybe read repeatedly.", location.AbsolutePath), "err", err)
				} else {
					s.Ctx.Logger().Infof("%s file deleted successfully", location.AbsolutePath)
				}
			}
			s.enumerator.RemoveDedupeLocations(s.state.PendingRemove[pendingCheckpointId])
			s.state.PendingCheckpoints = s.state.PendingCheckpoints[1:]
			delete(s.state.PendingRemove, pendingCheckpointId)
		}

	}
}

func FromSource[T any](env *stream.Environment, name string, withOptions ...WithOptions[T]) (stream.Stream[T], error) {
	o := &options[T]{}
	for _, withOptionsFn := range withOptions {
		if err := withOptionsFn(o); err != nil {
			return nil, err
		}
	}

	return stream.FormSource[T](env, stream.SourceStreamOptions[T]{
		Name: name,
		Source: &source[T]{
			FormatFn:           o.formatFn,
			delim:              o.delim,
			ComparatorFn:       o.comparatorFn,
			TimestampFn:        o.timestampFn,
			dir:                o.dir,
			pattern:            o.pattern,
			enablePeriodicScan: o.periodicScan,
		},
	})
}
