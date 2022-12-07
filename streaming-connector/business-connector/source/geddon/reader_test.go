package geddon

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
)

type lockFn struct {
	index int64
	fn    func() error
}

type testCollector[T any] struct {
	index    int64
	expected []T
	locks    []lockFn
	t        *testing.T
	mutex    *sync.Mutex
}

func (c *testCollector[T]) EmitEvent(event *element.Event[T]) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, lock := range c.locks {
		if c.index == lock.index {
			assert.Nil(c.t, lock.fn())
		}
	}
	assert.Equal(c.t, event.Value, c.expected[c.index])
	c.index += 1
}

func (c *testCollector[T]) EmitWatermark(watermark element.Watermark) {

}

func (c *testCollector[T]) EmitWatermarkStatus(statusType element.WatermarkStatus) {

}

func NewTestCollector[T any](t *testing.T, expected []T, fn ...lockFn) element.Collector[T] {
	return &testCollector[T]{
		index:    0,
		expected: expected,
		locks:    fn,
		t:        t,
		mutex:    &sync.Mutex{},
	}
}

func TestReader_Read(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	var reader *Reader[string]
	reader = NewReader(&mockContext{}, done, NewTestCollector(t, []string{"123123", "123123"}), func(filename string, data []byte) string {
		return strings.TrimSpace(string(data))
	}, '\n', nil)
	dir := path.Join(os.TempDir(), RandStr(10))
	_ = os.Mkdir(dir, os.ModePerm)
	filePath := path.Join(dir, "1.log")
	create, err := os.Create(filePath)
	create.WriteString("123123\n")
	create.WriteString("123123\n")
	err = create.Close()
	assert.Nil(t, err)
	err = reader.Read(Location{AbsolutePath: filePath})
	assert.Nil(t, err)
}

func TestReader_SnapshotState(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	dir := path.Join(os.TempDir(), RandStr(10))
	_ = os.Mkdir(dir, os.ModePerm)
	filePath := path.Join(dir, "1.log")
	var reader *Reader[string]
	reader = NewReader(&mockContext{}, done, NewTestCollector(t, []string{"123123", "123123"}, lockFn{
		index: 0,
		fn: func() error {
			location := reader.SnapshotState()
			if location.AbsolutePath != filePath {
				return fmt.Errorf("absolute path error")
			}
			i := len([]byte("123123\n"))
			if location.Offset != int64(i) {
				return fmt.Errorf("offset error")
			}
			return nil
		},
	}), func(filename string, data []byte) string {
		return strings.TrimSpace(string(data))
	}, '\n', nil)

	create, err := os.Create(filePath)
	create.WriteString("123123\n")
	create.WriteString("123123\n")
	err = create.Close()
	assert.Nil(t, err)
	err = reader.Read(Location{AbsolutePath: filePath})
	assert.Nil(t, err)
}
