package geddon

import (
	"bufio"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/pkg/errors"
	"io"
	"os"
)

type Reader[OUT any] struct {
	logger log.Logger

	formatFn        FormatFn[OUT]
	delim           byte
	timestampFn     TimestampFn
	collector       element.Collector[OUT]
	closed          bool
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
			line     []byte
		)

		for i := int64(0); err != io.EOF && !r.closed; i++ {
			line, err = r.currentReader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return errors.WithMessagef(err, "failed to read file")
			}
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

func (r *Reader[OUT]) Close() error {
	if r.closed {
		return errors.New("reader already closed")
	} else {
		r.closed = true
		return nil
	}
}

func NewReader[OUT any](ctx Context, done <-chan struct{}, collector element.Collector[OUT],
	formatFn FormatFn[OUT], delim byte, timestampFn TimestampFn) *Reader[OUT] {
	reader := &Reader[OUT]{
		logger:          ctx.Logger().Named("reader"),
		formatFn:        formatFn,
		timestampFn:     timestampFn,
		collector:       collector,
		closed:          false,
		delim:           delim,
		currentLocation: emptyLocation,
		currentFile:     nil,
		currentReader:   nil,
	}
	go func() {
		<-done
		reader.closed = true
	}()
	return reader
}
