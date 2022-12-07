package geddon

import (
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strconv"
	"testing"
	"time"
)

type mockContext struct {
}

func (m *mockContext) Logger() log.Logger {
	return log.Global().Named("mock")
}

func (m *mockContext) Store() store.Controller {
	return nil
}

func (m *mockContext) TimerManager() *TimerManager {
	return nil
}

func (m *mockContext) Call(f func()) {
	return
}

func TestEnumerator_AddLocation(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	dir := os.TempDir()
	t.Run("test slice queue FIFO", func(t *testing.T) {
		enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*"), 0, nil)
		enumerator.AddLocation(Location{AbsolutePath: "1"})
		enumerator.AddLocation(Location{AbsolutePath: "2"})
		enumerator.AddLocation(Location{AbsolutePath: "3"})
		enumerator.AddLocation(Location{AbsolutePath: "4"})
		location, ok := enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "1"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "2"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "3"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "4"}, location)
		assert.True(t, ok)
		go func() {
			time.Sleep(1 * time.Second)
			enumerator.closed = true
			enumerator.cond.Broadcast()
		}()
		location, ok = enumerator.Next()

		assert.False(t, ok)
	})

	t.Run("test priority queue", func(t *testing.T) {
		enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*"), 0, func(left Location, right Location) bool {
			leftAtoi, _ := strconv.Atoi(left.AbsolutePath)
			rightAtoi, _ := strconv.Atoi(right.AbsolutePath)
			return leftAtoi < rightAtoi
		})
		enumerator.AddLocation(Location{AbsolutePath: "4"})
		enumerator.AddLocation(Location{AbsolutePath: "2"})
		enumerator.AddLocation(Location{AbsolutePath: "3"})
		enumerator.AddLocation(Location{AbsolutePath: "1"})
		location, ok := enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "4"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "3"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "2"}, location)
		assert.True(t, ok)
		location, ok = enumerator.Next()
		assert.Equal(t, Location{AbsolutePath: "1"}, location)
		assert.True(t, ok)
		go func() {
			time.Sleep(1 * time.Second)
			enumerator.closed = true
			enumerator.cond.Broadcast()
		}()
		location, ok = enumerator.Next()

		assert.False(t, ok)
	})

}

func RandStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func TestEnumerator_ScanDir(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	t.Run("test slice queue FIFO", func(t *testing.T) {
		dir := path.Join(os.TempDir(), RandStr(10))
		_ = os.Mkdir(dir, os.ModePerm)
		enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*"), 0, nil)
		for i := 0; i < 10; i++ {
			create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.log", i)))
			assert.Nil(t, err)
			_ = create.Close()
		}
		enumerator.ScanDir()
		for i := 0; i < 10; i++ {
			location, b := enumerator.Next()
			assert.True(t, b)
			assert.Equal(t, location, Location{AbsolutePath: path.Join(dir, fmt.Sprintf("%d.log", i))})
		}
	})
	t.Run("test priority queue", func(t *testing.T) {
		dir := path.Join(os.TempDir(), RandStr(10))
		_ = os.Mkdir(dir, os.ModePerm)
		enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*"), 0, func(left Location, right Location) bool {
			leftAtoi, _ := strconv.Atoi(left.AbsolutePath)
			rightAtoi, _ := strconv.Atoi(right.AbsolutePath)
			return leftAtoi < rightAtoi
		})
		for i := 0; i < 10; i++ {
			create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.log", i)))
			assert.Nil(t, err)
			_ = create.Close()
		}
		enumerator.ScanDir()
		for i := 9; i >= 0; i-- {
			location, b := enumerator.Next()
			assert.True(t, b)
			assert.Equal(t, location, Location{AbsolutePath: path.Join(dir, fmt.Sprintf("%d.log", i))})
		}
	})
}

func TestEnumerator_Match(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	dir := path.Join(os.TempDir(), RandStr(10))
	_ = os.Mkdir(dir, os.ModePerm)
	enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*\\.log"), 0, func(left Location, right Location) bool {
		leftAtoi, _ := strconv.Atoi(left.AbsolutePath)
		rightAtoi, _ := strconv.Atoi(right.AbsolutePath)
		return leftAtoi < rightAtoi
	})
	for i := 0; i < 10; i++ {
		create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.log", i)))
		assert.Nil(t, err)
		_ = create.Close()
	}
	for i := 0; i < 10; i++ {
		create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.t", i)))
		assert.Nil(t, err)
		_ = create.Close()
	}
	enumerator.ScanDir()
	for i := 9; i >= 0; i-- {
		location, b := enumerator.Next()
		assert.True(t, b)
		assert.Equal(t, location, Location{AbsolutePath: path.Join(dir, fmt.Sprintf("%d.log", i))})
	}
	go func() {
		time.Sleep(1 * time.Second)
		enumerator.closed = true
		enumerator.cond.Broadcast()
	}()
	location, b := enumerator.Next()
	assert.False(t, b)
	assert.Equal(t, emptyLocation, location)
}

func TestEnumerator_StartWatchDir(t *testing.T) {
	log.Setup(log.DefaultOptions())
	done := make(chan struct{})
	dir := path.Join(os.TempDir(), RandStr(10))
	_ = os.Mkdir(dir, os.ModePerm)
	enumerator := NewEnumerator(&mockContext{}, done, dir, regexp.MustCompile(".*\\.log"), 0, func(left Location, right Location) bool {
		leftAtoi, _ := strconv.Atoi(left.AbsolutePath)
		rightAtoi, _ := strconv.Atoi(right.AbsolutePath)
		return leftAtoi < rightAtoi
	})
	err := enumerator.StartWatchDir()
	assert.Nil(t, err)
	t.Run("test watch", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.log", i)))
			assert.Nil(t, err)
			_ = create.Close()
		}
		time.Sleep(3 * time.Second)
		for i := 9; i >= 0; i-- {
			location, b := enumerator.Next()
			assert.True(t, b)
			assert.Equal(t, location, Location{AbsolutePath: path.Join(dir, fmt.Sprintf("%d.log", i))})
		}

	})
	t.Run("test remove dir and create agent", func(t *testing.T) {

		err = os.RemoveAll(dir)

		assert.Nil(t, err)
		err = os.Mkdir(dir, os.ModePerm)
		assert.Nil(t, err)
		enumerator.dedupeMap = map[string]struct{}{}
		//_ = enumerator.watcher.Close()
		time.Sleep(3 * time.Second)
		for i := 0; i < 10; i++ {
			create, err := os.Create(path.Join(dir, fmt.Sprintf("%d.log", i)))
			assert.Nil(t, err)
			_ = create.Close()
		}
		time.Sleep(3 * time.Second)
		for i := 9; i >= 0; i-- {
			location, b := enumerator.Next()
			assert.True(t, b)
			assert.Equal(t, location, Location{AbsolutePath: path.Join(dir, fmt.Sprintf("%d.log", i))})
		}
	})

}
