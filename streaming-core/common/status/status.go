package status

import "sync/atomic"

type Status int64

func (s Status) Ready() bool {
	return s == Ready
}
func (s Status) Running() bool {
	return s == Running
}
func (s Status) Closed() bool {
	return s == Closed
}

const (
	Ready Status = iota
	Running
	Closed
)

func CAP(statusPointer *Status, from, to Status) bool {
	return atomic.CompareAndSwapInt64((*int64)(statusPointer), int64(from), int64(to))
}
