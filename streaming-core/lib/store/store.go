package store

type State interface {
	Initialized() bool
	Mirror() MirrorState
	Clear()
}

type StateType string

const (
	ValueType = "value"
	MapType   = "map"
)

type MirrorState struct {
	_type  StateType
	_bytes []byte
}

func (m MirrorState) Initialized() bool   { return false }
func (m MirrorState) Mirror() MirrorState { return m }
func (m MirrorState) Clear()              {}
func (m MirrorState) Type() StateType     { return m._type }
func (m MirrorState) Bytes() []byte       { return m._bytes }

type Manager interface {
	Range(func(key string, state State) bool)
	Load(key string) (State, bool)
	Store(key string, state State)
	Delete(key string)
	Save(id int64) error
	Clean() error
	Persist(id int64) error
}
