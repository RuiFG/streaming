package geddon

import (
	"github.com/RuiFG/streaming/streaming-core/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/fsnotify/fsnotify"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type Enumerator struct {
	logger       log.Logger
	dir          string
	pattern      *regexp.Regexp
	scanDuration time.Duration

	closed bool
	cond   *sync.Cond

	dedupeMap map[string]struct{}
	//sorted locations
	queue Queue

	watcher *fsnotify.Watcher
	done    <-chan struct{}
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
	return e.queue.PopLocation(), true
}

func (e *Enumerator) ScanDir() {
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
}

func (e *Enumerator) StartPeriodicScanDir() {
	tick := time.Tick(e.scanDuration)
	go func() {
		for true {
			select {
			case tickTime := <-tick:
				e.logger.Debugf("%d start scanning dir", tickTime.UnixMilli())
				e.ScanDir()
			case <-e.done:
				return
			}
		}
	}()
}

func (e *Enumerator) WatchDir() (err error) {
	e.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err = e.watcher.Add(e.dir); err != nil {
		_ = e.watcher.Close()
		return err
	}
	return nil
}

func (e *Enumerator) StartWatchDir() error {
	if err := e.WatchDir(); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-e.done:
				return
			case event := <-e.watcher.Events:
				switch {
				case event.Has(fsnotify.Create):
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
				case event.Has(fsnotify.Remove) && (event.Name == "" || event.Name == e.dir || event.Name == path.Base(e.dir)):
					//re add
					for !e.closed {
						_ = e.watcher.Close()
						e.logger.Errorw("watching dir has been deleted, trying to re-watch dir...")
						if err := e.WatchDir(); err == nil {
							break
						}
						time.Sleep(1 * time.Second)
					}

				default:
					e.logger.Debugf("received other events %+v", event)
				}
			case err := <-e.watcher.Errors:
				e.logger.Warnw("received watcher errors %+v", err)
				for _ = e.watcher.Close(); err != nil && !e.closed; time.Sleep(1 * time.Second) {
					e.logger.Errorw("watcher failed, trying to re-watch dir...", "err", err)
					err = e.WatchDir()
				}
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
		e.queue.PushLocation(location)
		e.cond.Signal()
	}
}

func (e *Enumerator) AddDedupeLocations(locations []Location) {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	for _, location := range locations {
		e.dedupeMap[location.AbsolutePath] = struct{}{}
	}
}

func (e *Enumerator) RemoveDedupeLocations(locations []Location) {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	for _, location := range locations {
		delete(e.dedupeMap, location.AbsolutePath)
	}
}

func NewEnumerator(ctx Context, done <-chan struct{},
	dir string, pattern *regexp.Regexp,
	scanDuration time.Duration, fn ComparatorFn) *Enumerator {
	var queue Queue
	if fn == nil {
		queue = &SliceQueue{}
	} else {
		queue = &PriorityQueue{
			items:      nil,
			comparator: fn,
		}
	}
	enumerator := &Enumerator{
		logger:       ctx.Logger().Named("enumerator"),
		dir:          dir,
		pattern:      pattern,
		scanDuration: scanDuration,
		closed:       false,
		cond:         sync.NewCond(&sync.Mutex{}),
		dedupeMap:    map[string]struct{}{},
		queue:        queue,
		done:         done}
	go func() {
		<-done
		enumerator.closed = true
		enumerator.cond.Broadcast()
	}()
	return enumerator
}
