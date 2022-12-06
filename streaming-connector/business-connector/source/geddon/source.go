package geddon

import (
	"bufio"
	"container/heap"
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/stream"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type options[T any] struct {
	formatFn    FormatFn[T]
	timestampFn TimestampFn
	lessFn      LessFn
	dir         string
	pattern     *regexp.Regexp

	periodicScan time.Duration
}

type WithOptions[T any] func(options *options[T]) error

func WithFormat[T any](fn FormatFn[T]) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.formatFn = fn
		return nil
	}
}

func WithTimestamp[T any](fn TimestampFn) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.timestampFn = fn
		return nil
	}
}

func WithDir[T any](dir string, pattern *regexp.Regexp, fn LessFn) WithOptions[T] {
	return func(opts *options[T]) error {
		if dir == "" {
			return errors.New("dir can't be nil")
		}
		if fn == nil {
			return errors.New("enum fn can't be nil")
		}
		if pattern == nil {
			pattern = regexp.MustCompile(".*")
		}
		opts.dir = dir
		opts.pattern = pattern
		opts.lessFn = fn
		return nil
	}
}

func WithPeriodicScan[T any](duration time.Duration) WithOptions[T] {
	return func(opts *options[T]) error {
		opts.periodicScan = duration
		return nil
	}
}

type State struct {
	readingLocation Location

	PendingCheckpoints []int64
	//map[checkpointId][]ReadiedFilenames
	PendingRemove map[int64][]Location
}

type FormatFn[OUT any] func(filename string, line string) OUT

type TimestampFn func(filename string, line string) int64

type LessFn func(left Location, right Location) bool

type Location struct {
	AbsolutePath string
	Offset       int64
}

func (f Location) Filename() string {
	return path.Base(f.AbsolutePath)
}

func (f Location) Ext() string {
	return path.Ext(f.AbsolutePath)
}

var emptyLocation = Location{Offset: 0, AbsolutePath: ""}

type Reader[OUT any] struct {
	logger log.Logger

	formatFn    FormatFn[OUT]
	timestampFn TimestampFn
	collector   element.Collector[OUT]

	currentLocation Location
	currentFile     *os.File
	currentReader   *bufio.Reader
}

func (r *Reader[OUT]) Read(lt Location) error {
	var err error
	if r.currentFile, err = os.Open(lt.AbsolutePath); err != nil {
		return errors.WithMessagef(err, "can't open file")
	} else {
		defer func() {
			if err = r.currentFile.Close(); err != nil {
				r.logger.Warnw("close file error, memory leaks may occur", "err", err)
			}
			r.currentFile = nil
		}()

		if _, err = r.currentFile.Seek(lt.Offset, io.SeekStart); err != nil {
			return errors.WithMessagef(err, "can't reset offset to %d", lt.Offset)
		}
		r.currentLocation = lt
		r.currentReader = bufio.NewReaderSize(r.currentFile, 20*1024*1024)
		var (
			filename = lt.Filename()
			line     string
		)
		for i := int64(0); err != io.EOF; i++ {
			line, err = r.currentReader.ReadString('\n')
			if err != nil && err != io.EOF {
				return errors.WithMessagef(err, "read file failed")
			}
			line = strings.TrimSpace(line)
			var e *element.Event[OUT]
			if r.timestampFn != nil {
				e = &element.Event[OUT]{
					Value:        r.formatFn(filename, line),
					Timestamp:    r.timestampFn(filename, line),
					HasTimestamp: true,
				}
			} else {
				e = &element.Event[OUT]{
					Value:        r.formatFn(filename, line),
					Timestamp:    0,
					HasTimestamp: false,
				}
			}
			r.collector.EmitEvent(e)
		}
	}
	return nil
}

func (r *Reader[OUT]) SnapshotState() Location {
	if r.currentFile == nil {
		return emptyLocation
	}
	if seek, err := r.currentFile.Seek(0, io.SeekCurrent); err != nil {
		r.logger.Warnw("can't seek current file", "err", err)
		return emptyLocation
	} else {
		return Location{AbsolutePath: r.currentLocation.AbsolutePath, Offset: seek - int64(r.currentReader.Buffered())}
	}
}

type queue struct {
	items  []Location
	lessFn LessFn
}

func (q *queue) Len() int {
	return len(q.items)
}

func (q *queue) Less(i, j int) bool {
	return q.lessFn(q.items[i], q.items[j])
}

func (q *queue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *queue) Push(x any) {
	item := x.(Location)
	q.items = append(q.items, item)
}

func (q *queue) Pop() any {
	old := q.items
	n := len(old)
	x := old[n-1]
	q.items = old[0 : n-1]
	return x
}

type Enumerator struct {
	logger log.Logger

	dir     string
	pattern *regexp.Regexp

	scanDuration time.Duration

	closed bool
	cond   *sync.Cond

	dedupeMap map[string]struct{}
	//sorted locations
	queue *queue

	watcher *fsnotify.Watcher
	done    chan struct{}
}

func (e *Enumerator) Next() (Location, bool) {
	e.cond.L.Lock()
	for e.queue.Len() <= 0 && !e.closed {
		e.cond.Wait()
	}
	defer e.cond.L.Unlock()
	if e.closed {
		return emptyLocation, false
	}
	return heap.Pop(e.queue).(Location), true
}

func (e *Enumerator) StartPeriodicScanDir() error {
	tick := time.Tick(e.scanDuration)
	go func() {
		for true {
			select {
			case tickTime := <-tick:
				e.logger.Debugf("%d start scanning dir", tickTime.UnixMilli())
				if err := filepath.Walk(e.dir, func(filePath string, info fs.FileInfo, err error) error {
					if !info.IsDir() {
						if !e.pattern.MatchString(path.Base(filePath)) {
							e.logger.Infof("%s don't match the pattern, it will not be processed.", filePath)
						} else {
							e.AddLocation(Location{AbsolutePath: filePath, Offset: 0})
						}
					}
					return nil
				}); err != nil {
					e.logger.Warnw("failed to scan dir", "dir", e.dir, "err", err)
				}
			case <-e.done:
				return
			}
		}
	}()
	return nil
}

func (e *Enumerator) StartWatchDir() (err error) {
	e.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err = e.watcher.Add(e.dir); err != nil {
		_ = e.watcher.Close()
		return err
	}
	go func() {
		for {
			select {
			case <-e.done:
				return
			case event := <-e.watcher.Events:
				if event.Has(fsnotify.Create) {
					info, err := os.Stat(event.Name)
					if err != nil {
						continue
					} else {
						if !info.IsDir() {
							e.logger.Infof("listening for new files: %s.", event.Name)
							if !e.pattern.MatchString(path.Base(event.Name)) {
								e.logger.Infof("%s don't match the pattern, it will not be processed.", event.Name)
								break
							} else {
								e.AddLocation(Location{AbsolutePath: event.Name, Offset: 0})
							}
						}
					}

				}
			case err = <-e.watcher.Errors:
				e.logger.Warnw("watch file system failed.", "err", err)
			}
		}
	}()
	return nil
}

func (e *Enumerator) AddLocation(location Location) {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	if _, ok := e.dedupeMap[location.AbsolutePath]; !ok {
		e.dedupeMap[location.AbsolutePath] = struct{}{}
		heap.Push(e.queue, location)
		e.cond.Signal()
	}
}

func (e *Enumerator) RemoveLocations(locations []Location) {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	for _, location := range locations {
		delete(e.dedupeMap, location.AbsolutePath)
	}
}

func (e *Enumerator) Close() error {
	if e.closed {
		return errors.New("enumerator already closed")
	} else {
		e.closed = true
		//notify all those who are waiting for processing
		e.cond.Broadcast()
		close(e.done)
		return nil
	}
}

type source[OUT any] struct {
	BaseOperator[any, any, OUT]
	FormatFn[OUT]
	LessFn
	TimestampFn
	//dir is a log directory that needs to be watch and scan,
	//whenever a file is created in the directory, it will be read and emit to the downstream by line.
	dir string
	//it is processed only if the log file name matches the pattern.
	pattern *regexp.Regexp

	//periodic scan log dir path, if set 0, will not be enabled
	enablePeriodicScan time.Duration

	done chan struct{}
	//state is map[currentFilename]offset
	//the reading of the current file is complete if offset=-1
	state            *State
	readiedLocations []Location
	*Reader[OUT]
	*Enumerator
}

func (s *source[OUT]) NotifyCheckpointCome(checkpointId int64) {
	//1. truncate the current offset
	//2. add checkpoints to the pending list
	//3. save the list of files that have been read at the current checkpoint
	//4. clear the readied locations list
	s.state.readingLocation = s.Reader.SnapshotState()
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
			s.Enumerator.RemoveLocations(s.state.PendingRemove[pendingCheckpointId])
			s.state.PendingCheckpoints = s.state.PendingCheckpoints[1:]
			delete(s.state.PendingRemove, pendingCheckpointId)
		}

	}
}

func (s *source[OUT]) Open(ctx Context, collector element.Collector[OUT]) error {
	if err := s.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	if fileStateController, err := store.GobRegisterOrGet[State](s.Ctx.Store(), "state",
		func() State {
			return State{
				readingLocation:    emptyLocation,
				PendingCheckpoints: nil,
				PendingRemove:      map[int64][]Location{},
			}
		}, func(bytes []byte, err error) ([]byte, error) {
			return bytes, err
		}, func(state State, err error) (State, error) {
			return state, err
		}); err != nil {
		return err
	} else {
		s.state = fileStateController.Pointer()
	}
	s.done = make(chan struct{})
	s.Reader = &Reader[OUT]{
		logger:      ctx.Logger().Named("reader"),
		formatFn:    s.FormatFn,
		timestampFn: s.TimestampFn,
		collector:   collector,
	}

	s.Enumerator = &Enumerator{
		logger:       ctx.Logger().Named("enumerator"),
		dir:          s.dir,
		pattern:      s.pattern,
		scanDuration: s.enablePeriodicScan,
		closed:       false,
		cond:         sync.NewCond(&sync.Mutex{}),
		dedupeMap:    map[string]struct{}{},
		queue: &queue{
			items:  nil,
			lessFn: s.LessFn,
		},
		done: make(chan struct{}),
	}
	if len(s.state.PendingCheckpoints) > 0 {
		lastCheckpointId := s.state.PendingCheckpoints[len(s.state.PendingCheckpoints)-1]
		recoverFileLocations := s.state.PendingRemove[lastCheckpointId]
		s.state.PendingCheckpoints = s.state.PendingCheckpoints[0 : len(s.state.PendingCheckpoints)-1]
		delete(s.state.PendingRemove, lastCheckpointId)
		for _, location := range recoverFileLocations {
			s.Enumerator.AddLocation(location)
		}
	}
	if s.state.readingLocation != emptyLocation {
		s.Enumerator.AddLocation(s.state.readingLocation)
	}

	s.Enumerator.StartWatchDir()
	if s.enablePeriodicScan > 0 {
		s.Enumerator.StartPeriodicScanDir()
	}
	return nil
}

func (s *source[OUT]) Close() error {
	if s.done != nil {
		close(s.done)
		_ = s.Enumerator.Close()
	}
	return nil
}

func (s *source[OUT]) Run() {
	for {
		select {
		case <-s.done:
			return
		default:
			if location, ok := s.Enumerator.Next(); ok {
				if err := s.Reader.Read(location); err != nil {
					s.Ctx.Logger().Warnw("can't read file Location.", "Location", location)
				} else {
					s.readiedLocations = append(s.readiedLocations, location)
				}
			} else {
				continue
			}

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
			LessFn:             o.lessFn,
			TimestampFn:        o.timestampFn,
			dir:                o.dir,
			pattern:            o.pattern,
			enablePeriodicScan: o.periodicScan,
		},
	})
}
