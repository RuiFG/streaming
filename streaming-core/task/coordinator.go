package task

import (
	_c "context"
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"sync"
	"time"
)

type pendingBarrier struct {
	element.Detail
	isDiscarded    bool
	notYetAckTasks map[string]bool
}

func newPendingBarrier(detail element.Detail, tasksToWaitFor []Task) *pendingBarrier {
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
	element.Detail
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
	tasksToTrigger       []Task
	tasksToWaitFor       []Task
	pendingBarriers      *sync.Map
	completedCheckpoints *barrierStore
	interval             time.Duration
	cleanThreshold       int

	ticker             *time.Ticker
	barrierSignalChan  chan element.Signal
	barrierTriggerChan chan element.BarrierType
	storeBackend       store.Backend
	activated          bool
}

func (c *Coordinator) Activate() {
	c.logger.Infof("start coordinator")
	c.ticker = time.NewTicker(c.interval)
	tc := c.ticker.C
	go func() {
		c.activated = true
		toBeClean := 0
		defaultBarrierType := element.CheckpointBarrier
		barrierTriggerFn := func(detail element.Detail) {
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
					c.logger.Infof("task %s is not running, cancel it.", _task.Name())
					return
				}
			}

			pending := newPendingBarrier(detail, c.tasksToWaitFor)
			c.logger.Debugf("create pendingBarrier %d", detail.Id)
			c.pendingBarriers.Store(detail, pending)
			//let the sources send out a barrier
			for _, r := range c.tasksToTrigger {
				go func(_task Task) {
					_task.TriggerBarrier(detail)
				}(r)
			}
			toBeClean++
			if toBeClean >= c.cleanThreshold {
				_ = c.storeBackend.Clean()
				toBeClean = 0
			}

		}

		barrierSignalHandlerFn := func(signal element.Signal) {
			switch signal.Message {
			case element.ACK:
				c.logger.Debugf("receive ack from %s for id %d", signal.Name, signal.Id)
				if cp, ok := c.pendingBarriers.Load(signal.Detail); ok {
					checkpoint := cp.(*pendingBarrier)
					checkpoint.ack(signal.Name)
					if checkpoint.isFullyAck() {
						c.complete(signal.Detail)
					}
				} else {
					c.logger.Debugf("receive ack from %s for non existing id %d", signal.Name, signal.Id)
				}
			case element.DEC:
				c.logger.Debugf("receive dec from %s for id %d, cancel it", signal.Name, signal.Id)
				c.cancel(signal.Detail)
			}
		}

		for {
			select {
			case n := <-tc:
				barrierTriggerFn(element.Detail{Id: n.UnixMilli(), BarrierType: defaultBarrierType})
			case barrierType := <-c.barrierTriggerChan:
				if defaultBarrierType == element.ExitpointBarrier {
					barrierType = element.ExitpointBarrier
				} else {
					if barrierType == element.ExitpointBarrier {
						defaultBarrierType = element.ExitpointBarrier
					}
				}
				barrierTriggerFn(element.Detail{Id: time.Now().UnixMilli(), BarrierType: barrierType})
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
	c.barrierTriggerChan <- element.ExitpointBarrier
}

func (c *Coordinator) Wait() {
	<-c.ctx.Done()
	return
}

//cancel removes the barrier.Detail from a pendingBarrier
func (c *Coordinator) cancel(detail element.Detail) {
	if checkpoint, ok := c.pendingBarriers.Load(detail); ok {
		c.pendingBarriers.Delete(detail)
		checkpoint.(*pendingBarrier).dispose(true)
		c.notifyCancel(detail)
	} else {
		c.logger.Debugf("cancel for non existing checkpoint %d. just ignored", detail.Id)
	}
}

func (c *Coordinator) complete(detail element.Detail) {
	if ccp, ok := c.pendingBarriers.Load(detail); ok {
		err := c.storeBackend.Persist(detail.Id)
		if err != nil {
			c.logger.Warnf("cannot save checkpoint %d due to storage error: %v", detail.Id, err)
			return
		}
		c.completedCheckpoints.add(ccp.(*pendingBarrier).finalize())
		c.pendingBarriers.Delete(detail)
		//Drop the previous pendingBarriers
		c.pendingBarriers.Range(func(a1 interface{}, a2 interface{}) bool {
			cBarrier := a1.(element.Detail)
			cp := a2.(*pendingBarrier)
			if cBarrier.Id < detail.Id {
				cp.isDiscarded = true
				c.pendingBarriers.Delete(cBarrier)
			}
			return true
		})
		c.notifyComplete(detail)
		c.logger.Debugf("totally complete barrier %d", detail.Id)
		if detail.BarrierType == element.ExitpointBarrier {
			c.cancelFunc()
		}
	} else {
		c.logger.Infof("cannot find barrier %d to complete", detail.Id)
	}
}

func (c *Coordinator) notifyComplete(detail element.Detail) {
	for name, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyBarrierComplete(detail)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint complete.", "task", name, "err", err)
		}
	}
}

func (c *Coordinator) notifyCancel(detail element.Detail) {
	for _, _task := range c.tasksToWaitFor {
		if err := safe.Run(func() error {
			_task.NotifyBarrierCancel(detail)
			return nil
		}); err != nil {
			c.logger.Warnw("failed to notify checkpoint cancel.", "task", _task.Name(), "err", err)
		}
	}
}

func NewCoordinator(
	checkpointInterval time.Duration,
	tasksToTrigger []Task,
	tasksToWaitFor []Task,
	storeBackend store.Backend,
	barrierSignalChan chan element.Signal,
	barrierTriggerChan chan element.BarrierType,
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
		storeBackend:       storeBackend,
		cleanThreshold:     100,
	}

}
