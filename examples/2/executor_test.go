package main

import (
	"sync/atomic"
	"testing"
)

type Executor struct {
	exec func()
	//0:no process //1: executed //2: canceled
	status uint32
	done   chan struct{}
}

func (e *Executor) Cancel() bool {
	if atomic.CompareAndSwapUint32(&e.status, 0, 2) {
		close(e.done)
		return true
	} else {
		return false
	}
}

func (e *Executor) Canceled() bool {
	return e.status == 2
}

func (e *Executor) Exec() bool {
	if atomic.CompareAndSwapUint32(&e.status, 0, 1) {
		e.exec()
		close(e.done)
		return true
	} else {
		return false
	}
}

func (e *Executor) Done() <-chan struct{} {
	return e.done
}

func NewExecutor(exec func()) *Executor {
	return &Executor{
		exec:   exec,
		status: 0,
		done:   make(chan struct{}),
	}
}

func TestExecutor(t *testing.T) {
	executor := NewExecutor(func() {
		t.Log("exec")
	})
	go func() {
		<-executor.Done()
		executor.Exec()
	}()
	executor.Cancel()
	<-executor.Done()
}
