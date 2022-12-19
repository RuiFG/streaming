package store

import "sync"

type StateType int

const (
	NonParallelizeState StateType = iota
	ParallelizeState
)

type mirrorState struct {
	Type StateType
	//content must be Serializable
	Payload []byte
}

func (m mirrorState) mirror() (mirrorState, error) { return m, nil }

type State interface {
	mirror() (mirrorState, error)
}

type StateController[T any] interface {
	Pointer() *T
	Locker() *sync.RWMutex
	Clear()
}

type Controller interface {
	Range(func(key string, state State) bool)
	Load(key string) (State, bool)
	Store(key string, state State)
	Delete(key string)
}

type Manager interface {
	Controller(namespace string) Controller
	Save(id int64) error
	Clean() error
}

type Backend interface {
	Save(id int64, name string, state []byte) error
	Persist(checkpointId int64) error //Save the whole checkpoint state into storage
	Get(name string) ([]byte, error)
	Close() error
}
