package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"sync"
	"time"
)

type pendingBarrier struct {
	Barrier
	isDiscarded    bool
	notYetAckTasks map[string]bool
}

func newPendingBarrier(barrier Barrier, tasksToWaitFor []*Task) *pendingBarrier {
	pc := &pendingBarrier{Barrier: barrier}
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
	ccp := &completedBarrier{c.Barrier}
	return ccp
}

func (c *pendingBarrier) dispose(_ bool) {
	c.isDiscarded = true
}

type completedBarrier struct {
	Barrier
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

// Coordinator ensure Task barrier coordination
type Coordinator struct {
	ctx                  _c.Context
	cancelFunc           _c.CancelFunc
	logger               log.Logger
	tasksToTrigger       []*Task
	tasksToWaitFor       []*Task
	pendingBarriers      *sync.Map
	completedCheckpoints *barrierStore

	cleanThreshold int

	ticker             *time.Ticker
	barrierSignalChan  chan Signal
	barrierTriggerChan chan BarrierType
	storeBackend       store.Backend
	activated          bool
}

func (c *Coordinator) Activate() {
	c.logger.Infof("start coordinator")
	go func() {
		c.activated = true
		toBeClean := 0
		defaultBarrierType := CheckpointBarrier
		barrierTriggerFn := func(barrier Barrier) {
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
					c.logger.Infof("Task %s is not running, cancel it.", _task.Name())
					return
				}
			}

			pending := newPendingBarrier(barrier, c.tasksToWaitFor)
			c.logger.Debugf("create pendingBarrier %d", barrier.Id)
			c.pendingBarriers.Store(barrier, pending)
			//let the sources send out a barrier
			for _, r := range c.tasksToTrigger {
				go func(life *Task) {
					life.TriggerBarrier(barrier)
				}(r)
			}
			toBeClean++
			if toBeClean >= c.cleanThreshold {
				_ = c.storeBackend.Clean()
				toBeClean = 0
			}

		}

		barrierSignalHandlerFn := func(signal Signal) {
			switch signal.Message {
			case ACK:
				c.logger.Debugf("receive ack from %s for id %d", signal.Name, signal.Id)
				if cp, ok := c.pendingBarriers.Load(signal.Barrier); ok {
					checkpoint := cp.(*pendingBarrier)
					checkpoint.ack(signal.Name)
					if checkpoint.isFullyAck() {
						c.complete(signal.Barrier)
					}
				} else {
					c.logger.Debugf("receive ack from %s for non existing id %d", signal.Name, signal.Id)
				}
			case DEC:
				c.logger.Debugf("receive dec from %s for id %d, cancel it", signal.Name, signal.Id)
				c.cancel(signal.Barrier)
			}
		}

		for {
			select {
			case barrierType := <-c.barrierTriggerChan:
				if defaultBarrierType == ExitpointBarrier {
					barrierType = ExitpointBarrier
				} else {
					if barrierType == ExitpointBarrier {
						defaultBarrierType = ExitpointBarrier
					}
				}
				barrierTriggerFn(Barrier{Id: time.Now().UnixMilli(), BarrierType: barrierType})
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

// cancel removes the barrier.BarrierDetail from a pendingBarrier
func (c *Coordinator) cancel(barrier Barrier) {
	if checkpoint, ok := c.pendingBarriers.Load(barrier); ok {
		c.pendingBarriers.Delete(barrier)
		checkpoint.(*pendingBarrier).dispose(true)
		c.notifyCancel(barrier)
	} else {
		c.logger.Debugf("cancel for non existing checkpoint %d. just ignored", barrier.Id)
	}
}

func (c *Coordinator) complete(barrier Barrier) {
	if ccp, ok := c.pendingBarriers.Load(barrier); ok {
		err := c.storeBackend.Persist(barrier.Id)
		if err != nil {
			c.logger.Warnf("cannot save checkpoint %d due to storage error: %v", barrier.Id, err)
			return
		}
		c.completedCheckpoints.add(ccp.(*pendingBarrier).finalize())
		c.pendingBarriers.Delete(barrier)
		//Drop the previous pendingBarriers
		c.pendingBarriers.Range(func(a1 interface{}, a2 interface{}) bool {
			cBarrier := a1.(Barrier)
			cp := a2.(*pendingBarrier)
			if cBarrier.Id < barrier.Id {
				cp.isDiscarded = true
				c.pendingBarriers.Delete(cBarrier)
			}
			return true
		})
		c.notifyComplete(barrier)
		c.logger.Debugf("totally complete barrier %d", barrier.Id)
		if barrier.BarrierType == ExitpointBarrier {
			c.cancelFunc()
		}
	} else {
		c.logger.Infof("cannot find barrier %d to complete", barrier.Id)
	}
}

func (c *Coordinator) notifyComplete(barrier Barrier) {
	for name, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyBarrierComplete(barrier)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint complete.", "Task", name, "err", err)
		}
	}
}

func (c *Coordinator) notifyCancel(barrier Barrier) {
	for _, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyBarrierCancel(barrier)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint cancel.", "Task", _task.Name(), "err", err)
		}
	}
}

func NewCoordinator(
	tasksToTrigger []*Task,
	tasksToWaitFor []*Task,
	storeBackend store.Backend,
	barrierSignalChan chan Signal,
	barrierTriggerChan chan BarrierType,
) *Coordinator {
	ctx, cancelFunc := _c.WithCancel(_c.Background())
	return &Coordinator{
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		logger:          log.Global().Named("coordinator"),
		tasksToTrigger:  tasksToTrigger,
		tasksToWaitFor:  tasksToWaitFor,
		pendingBarriers: new(sync.Map),
		completedCheckpoints: &barrierStore{
			maxNum: 5,
		},
		barrierTriggerChan: barrierTriggerChan,
		barrierSignalChan:  barrierSignalChan,
		storeBackend:       storeBackend,
		cleanThreshold:     100,
	}

}
