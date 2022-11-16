package store

import (
	"fmt"
	"github.com/xujiajun/nutsdb"
	"strconv"
	"sync"
)

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

type fs struct {
	db          *nutsdb.DB
	store       *sync.Map
	checkpoints []int64
	max         int
}

func (r *fs) init() error {
	return r.db.Update(func(tx *nutsdb.Tx) error {
		var lastCheckpointId int64 = 0
		if err := tx.IterateBuckets(nutsdb.DataStructureBPTree, func(bucket string) {
			checkpointId, _ := strconv.ParseInt(bucket, 10, 64)
			if lastCheckpointId < checkpointId {
				lastCheckpointId = checkpointId
			}
		}); err != nil {
			return err
		}
		entries, err := tx.GetAll(strconv.FormatInt(lastCheckpointId, 10))
		if err != nil && err != nutsdb.ErrBucketEmpty {
			return err
		} else if len(entries) > 0 {
			r.checkpoints = append(r.checkpoints, lastCheckpointId)
			checkpointM := &sync.Map{}
			for _, entry := range entries {
				checkpointM.Store(string(entry.Key), entry.Value)
			}
			r.store.Store(lastCheckpointId, checkpointM)
		}
		return nil
	})

}

func (r *fs) Save(id int64, name string, state []byte) error {
	var checkpointM *sync.Map
	if tmp, ok := r.store.Load(id); !ok {
		checkpointM = &sync.Map{}
		r.store.Store(id, checkpointM)
	} else {
		if checkpointM, ok = tmp.(*sync.Map); !ok {
			return fmt.Errorf("invalid checkpoint %d with value %v: should be *sync.Apply type", id, tmp)
		}
	}
	checkpointM.Store(name, state)
	return nil
}

func (r *fs) Persist(id int64) error {
	if m, ok := r.store.Load(id); !ok {
		return fmt.Errorf("stateBackend for checkpoint %d not found", id)
	} else {
		r.checkpoints = append(r.checkpoints, id)

		for len(r.checkpoints) > r.max {
			cp := r.checkpoints[0]
			r.checkpoints = r.checkpoints[1:]
			r.store.Delete(cp)
		}
		if err := r.db.Update(func(tx *nutsdb.Tx) error {
			var err error
			m.(*sync.Map).Range(func(name, state any) bool {
				if err = tx.Put(
					strconv.FormatInt(id, 10), []byte(name.(string)), state.([]byte), 0); err != nil {
					_ = tx.Rollback()
					return false
				}
				return true
			})
			return err
		}); err != nil {
			return fmt.Errorf("save checkpoint err: %v", err)
		}
	}
	return nil
}

func (r *fs) Get(name string) ([]byte, error) {
	if len(r.checkpoints) > 0 {
		if v, ok := r.store.Load(r.checkpoints[len(r.checkpoints)-1]); ok {
			if checkpointM, ok := v.(*sync.Map); !ok {
				return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Apply", v, name)
			} else {
				if stateI, ok := checkpointM.Load(name); ok {
					switch state := stateI.(type) {
					case []byte:
						return state, nil
					default:
						return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not []byte", stateI, name)
					}
				}
			}
		} else {
			return nil, fmt.Errorf("stateBackend for checkpoint %d not found", r.checkpoints[len(r.checkpoints)-1])
		}
	}
	return nil, nil
}

func (r *fs) Clean() error {
	deleteCheckpointIds := make([]int64, 0)
	r.store.Range(func(key, value any) bool {
		checkpointId := key.(int64)
		if checkpointId != r.checkpoints[0] {
			deleteCheckpointIds = append(deleteCheckpointIds, checkpointId)
		}
		return true
	})
	for _, checkpointId := range deleteCheckpointIds {
		_ = r.db.Update(func(tx *nutsdb.Tx) error {
			return tx.DeleteBucket(nutsdb.DataStructureBPTree, strconv.FormatInt(checkpointId, 10))
		})
	}
	return nil
}

func (r *fs) Close() error {
	return r.db.Close()
}

func NewFs(stateDir string, max int) (Backend, error) {
	opts := nutsdb.DefaultOptions
	opts.Dir = stateDir
	db, err := nutsdb.Open(opts)
	if err != nil {
		return nil, err
	}
	store := &fs{max: max, store: &sync.Map{}, checkpoints: []int64{}, db: db}
	return store, store.init()
}
