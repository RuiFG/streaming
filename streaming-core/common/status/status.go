package status

import "sync/atomic"

type Status uint32

func (s *Status) CAS(from, to Status) bool {
	return atomic.CompareAndSwapUint32((*uint32)(s), uint32(from), uint32(to))
}

func (s *Status) Ready() bool {
	return *s == Ready
}

func (s *Status) Running() bool {
	return *s == Running
}

func (s *Status) Closing() bool {
	return *s == Closing
}

func (s *Status) Closed() bool {
	return *s == Closed
}

const (
	Ready Status = iota
	Running
	Closing
	Closed
)

func NewStatus(status Status) *Status {
	return &status
}
