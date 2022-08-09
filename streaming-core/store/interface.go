package store

type StateType string

const (
	ValueType = "value"
	MapType   = "map"
)

type mirrorState struct {
	StateType
	Bytes []byte
}

func (m mirrorState) Initialized() bool   { return false }
func (m mirrorState) mirror() mirrorState { return m }
func (m mirrorState) Clear()              {}

type State interface {
	Initialized() bool
	mirror() mirrorState
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
	Clean() error
	Close() error
}
