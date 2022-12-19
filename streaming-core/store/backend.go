package store

import (
	"fmt"
	"github.com/xujiajun/nutsdb"
	"go.uber.org/zap"
	"sort"
	"strconv"
	"sync"
)

func formatCheckpointId(id int64) string {
	return strconv.FormatInt(id, 10)
}

func parseCheckpointId(idStr string) int64 {
	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id
}

// memory only for test
type memory struct {
}

func (m *memory) Save(id int64, name string, state []byte) error { return nil }

func (m *memory) Persist(checkpointId int64) error { return nil }

func (m *memory) Get(name string) ([]byte, error) { return nil, nil }

func (m *memory) Close() error { return nil }

func NewMemoryBackend() Backend {
	return &memory{}
}

type fs struct {
	logger *zap.Logger
	db     *nutsdb.DB
	//storage stores all checkpoint state
	storage *sync.Map
	//checkpoints are currently completed checkpoint id sorted slice
	checkpoints []int64
	//checkpointsTotalNum
	checkpointsTotalNum    int
	checkpointsNumMerged   int
	checkpointsNumRetained int
}

func (r *fs) init() error {
	return r.db.View(func(tx *nutsdb.Tx) error {
		if err := tx.IterateBuckets(nutsdb.DataStructureBPTree, "*", func(key string) bool {
			r.checkpoints = append(r.checkpoints, parseCheckpointId(key))
			return true
		}); err != nil {
			return fmt.Errorf("unable to iterate checkpoint, the state maybe corrupted: %w", err)
		}
		sort.Slice(r.checkpoints, func(i, j int) bool {
			return r.checkpoints[i] < r.checkpoints[j]
		})
		for _, checkpointId := range r.checkpoints {
			if entries, err := tx.GetAll(formatCheckpointId(checkpointId)); err != nil {
				return fmt.Errorf("failed to get %d checkpoint state: %w", checkpointId, err)
			} else {
				if len(entries) > 0 {
					checkpointState := &sync.Map{}
					for _, entry := range entries {
						checkpointState.Store(string(entry.Key), entry.Value)
					}
					r.storage.Store(checkpointId, checkpointState)
				}
			}
		}
		return nil
	})

}

// Save state according to checkpoint and operator name
// if the checkpoint does not exist, will create
func (r *fs) Save(checkpointId int64, name string, state []byte) error {
	var checkpointState *sync.Map
	if tmp, ok := r.storage.Load(checkpointId); !ok {
		checkpointState = &sync.Map{}
		r.storage.Store(checkpointId, checkpointState)
	} else {
		checkpointState = tmp.(*sync.Map)
	}
	checkpointState.Store(name, state)
	return nil
}

// Persist checkpoint to db file
func (r *fs) Persist(checkpointId int64) error {
	if m, ok := r.storage.Load(checkpointId); !ok {
		return fmt.Errorf("checkpoint %d not found", checkpointId)
	} else {
		r.checkpoints = append(r.checkpoints, checkpointId)

		//1. persist checkpoint state into db
		if err := r.db.Update(func(tx *nutsdb.Tx) error {
			var err error
			m.(*sync.Map).Range(func(name, state any) bool {
				if err = tx.Put(
					formatCheckpointId(checkpointId), []byte(name.(string)), state.([]byte), 0); err != nil {
					return false
				}
				return true
			})
			return nil
		}); err != nil {
			return fmt.Errorf("failed to persist %d checkpoint state: %w", checkpointId, err)
		}
		r.checkpointsTotalNum += 1
		//2.clean up expired checkpoint status in db
		//3.clean up checkpoint status in memory
		if r.checkpointsTotalNum%r.checkpointsNumRetained == 0 {
			if err := r.db.Update(func(tx *nutsdb.Tx) error {
				var deletedCheckpointIds []int64
				if len(r.checkpoints) > r.checkpointsNumRetained {
					deletedCheckpointIds = r.checkpoints[:len(r.checkpoints)-r.checkpointsNumRetained]
					r.checkpoints = r.checkpoints[len(r.checkpoints)-r.checkpointsNumRetained:]
				}
				for _, deletedCheckpointId := range deletedCheckpointIds {
					if err := tx.DeleteBucket(nutsdb.DataStructureBPTree, formatCheckpointId(deletedCheckpointId)); err != nil {
						return err
					}
				}
				for _, deletedCheckpointId := range deletedCheckpointIds {
					r.storage.Delete(deletedCheckpointId)
				}
				return nil
			}); err != nil {
				r.logger.Warn("failed to clear up expired checkpoint data.", zap.Error(err))
			}
		}
		if r.checkpointsTotalNum%r.checkpointsNumMerged == 0 {
			//4.merge fs state
			if err := r.db.Merge(); err != nil {
				r.logger.Warn("failed to merge fs state.", zap.Error(err))
			}
		}

	}
	return nil
}

func (r *fs) Get(name string) ([]byte, error) {
	if len(r.checkpoints) > 0 {
		if v, ok := r.storage.Load(r.checkpoints[len(r.checkpoints)-1]); ok {
			if checkpointM, ok := v.(*sync.Map); !ok {
				return nil, fmt.Errorf("invalid state %v stored for operator %s: checkpoint state type is not *sync.Map", v, name)
			} else {
				if stateI, ok := checkpointM.Load(name); ok {
					switch state := stateI.(type) {
					case []byte:
						return state, nil
					default:
						return nil, fmt.Errorf("invalid state %v stored for operator %s: state type is not []byte", stateI, name)
					}
				}
			}
		} else {
			return nil, fmt.Errorf("state backend for checkpoint %d not found", r.checkpoints[len(r.checkpoints)-1])
		}
	}
	return nil, nil
}

func (r *fs) Close() error {
	return r.db.Close()
}

func NewFSBackend(logger *zap.Logger, checkpointsDir string, checkpointsNumRetained int, checkpointsNumMerged int) (Backend, error) {
	opts := nutsdb.DefaultOptions
	opts.SegmentSize = 1 * nutsdb.GB
	opts.Dir = checkpointsDir
	db, err := nutsdb.Open(opts)
	if err != nil {
		return nil, err
	}
	store := &fs{
		logger:                 logger,
		db:                     db,
		storage:                &sync.Map{},
		checkpoints:            []int64{},
		checkpointsNumRetained: checkpointsNumRetained,
		checkpointsNumMerged:   checkpointsNumMerged,
	}
	return store, store.init()
}
