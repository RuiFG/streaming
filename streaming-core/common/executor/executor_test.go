package executor

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExecutor_Cancel(t *testing.T) {
	executor := NewExecutor(func() {
		t.Errorf("can't happend")
	})
	assert.True(t, executor.Cancel())
	select {
	case <-executor.Done():
		t.Log("success")
	case <-time.After(2 * time.Millisecond):
		t.Errorf("can't happend")
	}
	assert.False(t, executor.Exec())
	assert.True(t, executor.Canceled())
}

func TestExecutor_Exec(t *testing.T) {
	var success = false
	executor := NewExecutor(func() {
		success = true
	})
	assert.True(t, executor.Exec())
	select {
	case <-executor.Done():
		t.Log("success")
	case <-time.After(2 * time.Millisecond):
		t.Errorf("can't happend")
	}
	assert.True(t, success)
	assert.False(t, executor.Cancel())

	assert.False(t, executor.Canceled())
}

func TestExecutor_execPanic(t *testing.T) {
	executor := NewExecutor(func() {
		panic("")
	})
	assert.Panics(t, func() {
		executor.Exec()
	})
	select {
	case <-executor.Done():
		t.Log("success")
	case <-time.After(2 * time.Millisecond):
		t.Errorf("can't happend")
	}
	assert.False(t, executor.Exec())
	assert.False(t, executor.Cancel())

	assert.False(t, executor.Canceled())
}
