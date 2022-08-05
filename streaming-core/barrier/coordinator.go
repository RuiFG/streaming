package barrier

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/safe"

	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"sync"
	"time"
)

type pendingBarrier struct {
	Detail
	isDiscarded    bool
	notYetAckTasks map[string]bool
}

func newPendingBarrier(detail Detail, tasksToWaitFor []task.Task) *pendingBarrier {
	pc := &pendingBarrier{Detail: detail}
	nyat := make(map[string]bool)
	for _, _task := range tasksToWaitFor {
		nyat[_task.Name()] = true
	}
	pc.notYetAckTasks = nyat
	return pc
}

func (c *pendingBarrier) ack(opId string) bool {
	if c.isDiscarded {
		return false
	}
	delete(c.notYetAckTasks, opId)
	//TODO serialize state
	return true
}

func (c *pendingBarrier) isFullyAck() bool {
	return len(c.notYetAckTasks) == 0
}

func (c *pendingBarrier) finalize() *completedBarrier {
	ccp := &completedBarrier{c.Detail}
	return ccp
}

func (c *pendingBarrier) dispose(_ bool) {
	c.isDiscarded = true
}

type completedBarrier struct {
	Detail
}

type barrierStore struct {
	maxNum   int
	barriers []*completedBarrier
}

func (s *barrierStore) add(c *completedBarrier) {
	s.barriers = append(s.barriers, c)
	if len(s.barriers) > s.maxNum {
		s.barriers = s.barriers[1:]
	}
}

//Coordinator ensure task barrier coordination
type Coordinator struct {
	ctx                  _c.Context
	cancelFunc           _c.CancelFunc
	logger               log.Logger
	tasksToTrigger       []task.Task
	tasksToWaitFor       []task.Task
	pendingBarriers      *sync.Map
	completedCheckpoints *barrierStore
	interval             time.Duration
	cleanThreshold       int

	ticker             *time.Ticker
	barrierSignalChan  chan Signal
	barrierTriggerChan chan Type
	storeManager       store.Manager
	activated          bool
}

func (c *Coordinator) Activate() {
	c.logger.Infof("start coordinator")
	c.ticker = time.NewTicker(c.interval)
	tc := c.ticker.C
	go func() {
		c.activated = true
		toBeClean := 0
		defaultBarrierType := CheckpointBarrier
		barrierTriggerFn := func(detail Detail) {
			//trigger barrier
			waitCheckpointNum := 0
			c.pendingBarriers.Range(func(key, value any) bool {
				waitCheckpointNum += 1
				return true
			})
			if waitCheckpointNum >= 5 {
				c.logger.Warnw("waiting barriers exceeds the maximum number:5, cancel it.")
				return
			}
			for _, _task := range c.tasksToWaitFor {
				if !_task.Running() {
					c.logger.Infof("task %barrierSignal is not running, cancel it.", _task.Name())
					return
				}
			}

			pending := newPendingBarrier(detail, c.tasksToWaitFor)
			c.logger.Debugf("create pendingBarrier %d", detail.Id)
			c.pendingBarriers.Store(detail, pending)
			//let the sources send out a barrier
			for _, r := range c.tasksToTrigger {
				go func(_task task.Task) {
					_task.TriggerBarrier(detail)
				}(r)
			}
			toBeClean++
			if toBeClean >= c.cleanThreshold {
				_ = c.storeManager.Clean()
				toBeClean = 0
			}

		}

		barrierSignalHandlerFn := func(signal Signal) {
			switch signal.Message {
			case ACK:
				c.logger.Debugf("receive ack from %barrierSignal for checkpoint %d", signal.Id)
				if cp, ok := c.pendingBarriers.Load(signal.Detail); ok {
					checkpoint := cp.(*pendingBarrier)
					checkpoint.ack(signal.Name)
					if checkpoint.isFullyAck() {
						c.complete(signal.Detail)
					}
				} else {
					c.logger.Debugf("receive ack from %barrierSignal for non existing checkpoint %d", signal.Name, signal.Id)
				}
			case DEC:
				c.logger.Debugf("receive dec from %barrierSignal for checkpoint %d, cancel it", signal.Name, signal.Id)
				c.cancel(signal.Detail)
			}
		}

		for {
			select {
			case n := <-tc:
				barrierTriggerFn(Detail{Id: n.UnixMilli(), Type: defaultBarrierType})
			case barrierType := <-c.barrierTriggerChan:
				if defaultBarrierType == ExitpointBarrier {
					barrierType = ExitpointBarrier
				} else {
					if barrierType == ExitpointBarrier {
						defaultBarrierType = ExitpointBarrier
					}
				}
				barrierTriggerFn(Detail{Id: time.Now().UnixMilli(), Type: barrierType})
			case barrierSignal := <-c.barrierSignalChan:
				barrierSignalHandlerFn(barrierSignal)
			case <-c.ctx.Done():
				c.logger.Info("cancelling coordinator....")
				c.ticker.Stop()
				c.logger.Info("stop coordinator ticker")
				return
			}
		}
	}()

}

func (c *Coordinator) Deactivate() {
	c.barrierTriggerChan <- ExitpointBarrier
}

func (c *Coordinator) Wait() {
	<-c.ctx.Done()
	return
}

//cancel removes the barrier.Detail from a pendingBarrier
func (c *Coordinator) cancel(detail Detail) {
	if checkpoint, ok := c.pendingBarriers.Load(detail); ok {
		c.pendingBarriers.Delete(detail)
		checkpoint.(*pendingBarrier).dispose(true)
		c.notifyCancel(detail)
	} else {
		c.logger.Debugf("cancel for non existing checkpoint %d. just ignored", detail.Id)
	}
}

func (c *Coordinator) complete(detail Detail) {
	if ccp, ok := c.pendingBarriers.Load(detail.Id); ok {
		err := c.storeManager.Persist(detail.Id)
		if err != nil {
			c.logger.Warnf("cannot save checkpoint %d due to storage error: %v", detail.Id, err)
			return
		}
		c.completedCheckpoints.add(ccp.(*pendingBarrier).finalize())
		c.pendingBarriers.Delete(detail.Id)
		//Drop the previous pendingBarriers
		c.pendingBarriers.Range(func(a1 interface{}, a2 interface{}) bool {
			cBarrier := a1.(Detail)
			cp := a2.(*pendingBarrier)
			if cBarrier.Id < detail.Id {
				cp.isDiscarded = true
				c.pendingBarriers.Delete(cBarrier)
			}
			return true
		})
		c.notifyComplete(detail)
		c.logger.Debugf("totally complete checkpoint %d", detail.Id)
		if detail.Type == ExitpointBarrier {
			c.cancelFunc()
		}
	} else {
		c.logger.Infof("cannot find checkpoint %d to complete", detail.Id)
	}
}

func (c *Coordinator) notifyComplete(detail Detail) {
	for name, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyComplete(detail)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint complete.", "task", name, "err", err)
		}
	}
}

func (c *Coordinator) notifyCancel(detail Detail) {
	for _, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyCancel(detail)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint cancel.", "task", _task.Name(), "err", err)
		}
	}
}

func NewCoordinator(
	checkpointInterval time.Duration,
	tasksToTrigger []task.Task,
	tasksToWaitFor []task.Task,
	storeManager store.Manager,
	barrierSignalChan chan Signal,
	barrierTriggerChan chan Type,
) *Coordinator {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Coordinator{
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		logger:          log.Named("coordinator"),
		tasksToTrigger:  tasksToTrigger,
		tasksToWaitFor:  tasksToWaitFor,
		pendingBarriers: new(sync.Map),
		completedCheckpoints: &barrierStore{
			maxNum: 3,
		},
		barrierTriggerChan: barrierTriggerChan,
		barrierSignalChan:  barrierSignalChan,
		interval:           checkpointInterval,
		storeManager:       storeManager,
		cleanThreshold:     100,
	}

}
