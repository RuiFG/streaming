package store

import (
	"github.com/RuiFG/streaming/streaming-core/store/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"sync"
)

type manager struct {
	mutex   *sync.Mutex
	mm      map[string]*controller
	name    string
	backend Backend
}

func (m *manager) init() error {
	if bytes, err := m.backend.Get(m.name); err != nil {
		return errors.WithMessagef(err, "failed to get %s state manager's state", m.name)
	} else {
		if bytes != nil {
			managerState := &pb.ManagerState{}
			if err := proto.Unmarshal(bytes, managerState); err != nil {
				return errors.WithMessagef(err, "failed to unmarshal %s state manager's state", m.name)
			}
			for namespace, controllerState := range managerState.Data {
				m.mm[namespace] = &controller{mm: &sync.Map{}}
				for name, stateV := range controllerState.Data {
					m.mm[namespace].Store(name, mirrorState{
						Type:    StateType(stateV.Type),
						Payload: stateV.Payload,
					})
				}
			}
		}
	}
	return nil
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

func (m *manager) Save(id int64) (err error) {
	managerState := &pb.ManagerState{Data: map[string]*pb.ControllerState{}}
	for namespace, control := range m.mm {
		if managerState.Data[namespace] == nil {
			managerState.Data[namespace] = &pb.ControllerState{Data: map[string]*pb.State{}}
		}
		control.Range(func(key string, state State) bool {
			var ms mirrorState
			if ms, err = state.mirror(); err != nil {
				return false
			} else {
				managerState.Data[namespace].Data[key] = &pb.State{
					Type:    int32(ms.Type),
					Payload: ms.Payload,
				}
				return true
			}
		})
		if err != nil {
			return errors.WithMessage(err, "failed to save data")
		}
	}
	if marshal, err := proto.Marshal(managerState); err != nil {
		return err
	} else {
		return m.backend.Save(id, m.name, marshal)
	}
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

func NewManager(name string, backend Backend) (Manager, error) {
	managerV := &manager{
		mutex:   &sync.Mutex{},
		mm:      map[string]*controller{},
		name:    name,
		backend: backend,
	}
	return managerV, managerV.init()
}
