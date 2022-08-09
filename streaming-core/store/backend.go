package store

type memory struct {
}

func (m *memory) Save(id int64, name string, state []byte) error { return nil }

func (m *memory) Persist(checkpointId int64) error { return nil }

func (m *memory) Get(name string) ([]byte, error) { return nil, nil }

func (m *memory) Clean() error { return nil }

func (m *memory) Close() error { return nil }

func NewMemoryBackend() Backend {
	return &memory{}
}
