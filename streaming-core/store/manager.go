package store

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type manager struct {
	mutex   *sync.Mutex
	mm      map[string]*controller
	name    string
	backend Backend
}

func (m *manager) Controller(namespace string) Controller {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if c, ok := m.mm[namespace]; ok {
		return c
	} else {
		c = &controller{&sync.Map{}}
		m.mm[namespace] = c
		return c
	}
}

func (m *manager) Save(id int64) error {
	mmm := map[string]map[string]mirrorState{}
	for namespace, c := range m.mm {
		if mmm[namespace] == nil {
			mmm[namespace] = map[string]mirrorState{}
		}
		c.Range(func(key string, state State) bool {
			mmm[namespace][key] = state.mirror()
			return true
		})
	}
	var buffer bytes.Buffer
	decoder := gob.NewEncoder(&buffer)
	if err := decoder.Encode(mmm); err != nil {
		return err
	}
	return m.backend.Save(id, m.name, buffer.Bytes())
}

func (m *manager) Clean() error {
	for _, c := range m.mm {
		c.Range(func(key string, state State) bool {
			c.Delete(key)
			return true
		})
	}
	return nil
}

func NewManager(name string, backend Backend) Manager {
	return &manager{
		mutex:   &sync.Mutex{},
		mm:      map[string]*controller{},
		name:    name,
		backend: backend,
	}
}
