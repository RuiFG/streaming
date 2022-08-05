package store

import "sync"

type manager struct {
	m *sync.Map
}

func (m *manager) Save(id int64) error {
	//TODO implement me
	panic("implement me")
}

func (m *manager) Clean() error {
	//TODO implement me
	panic("implement me")
}

func (m *manager) Persist(id int64) error {
	//TODO implement me
	panic("implement me")
}

func (m *manager) Range(fn func(key string, state State) bool) {
	m.m.Range(func(key, value any) bool {
		return fn(key.(string), value.(State))
	})
}

func (m *manager) Load(key string) (State, bool) {
	if load, ok := m.m.Load(key); !ok {
		return nil, ok
	} else {
		return load.(State), ok
	}
}

func (m *manager) Store(key string, state State) {
	m.m.Store(key, state)
}

func (m *manager) Delete(key string) {
	m.m.Delete(key)
}
