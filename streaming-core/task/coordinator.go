package task

import (
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"math"
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

func (c *pendingBarrier) ack(task string) bool {
	if c.isDiscarded {
		return false
	}
	delete(c.notYetAckTasks, task)
	return true
}

func (c *pendingBarrier) isFullyAck() bool {
	return len(c.notYetAckTasks) == 0
}

func (c *pendingBarrier) dispose() {
	c.isDiscarded = true
}

// Coordinator ensure Task barrier coordination
type Coordinator struct {
	status             status.Status
	done               chan struct{}
	logger             log.Logger
	tasksToTrigger     []*Task
	tasksToWaitFor     []*Task
	pendingBarriers    map[Barrier]*pendingBarrier
	lastPendingBarrier *pendingBarrier

	barrierSignalChan  chan Signal
	triggerBarrierChan chan struct{}
	storeBackend       store.Backend

	checkpointFailureNumber int

	tolerableCheckpointFailureNumber int
	maxConcurrentCheckpoints         int

	minPauseBetweenCheckpoints int64
	//TODO waiting for support
	checkpointTimeout time.Duration
}

func (c *Coordinator) Activate() {
	go func() {
		if status.CAP(&c.status, status.Ready, status.Running) {
			c.logger.Infof("started")
			var (
				//init impossible timer.
				lastBarrierTimeout = time.NewTimer(math.MaxInt64)
				lastBarrier        Barrier
			)
			for {
			loop:
				select {
				case <-c.triggerBarrierChan:
					timestamp := time.Now().UnixMilli()
					if c.status.Running() && c.lastPendingBarrier != nil &&
						timestamp < (c.lastPendingBarrier.CheckpointId+c.minPauseBetweenCheckpoints) {
						c.logger.Warnf("the time to trigger a checkpoint is less than the min pause time between two checkpoints, dec checkpoint.")
						goto loop
					}

					if len(c.pendingBarriers) > c.maxConcurrentCheckpoints {
						c.logger.Warnf("waiting barriers exceeds the maximum number:%d, dec it.", c.maxConcurrentCheckpoints)
						goto loop
					}

					for _, _task := range c.tasksToWaitFor {
						if !_task.Running() {
							c.logger.Warnf("task %s is not status, dec checkpoint.", _task.Name())
							goto loop
						}
					}
					lastBarrier = Barrier{CheckpointId: timestamp}
					lastBarrierTimeout.Stop()
					lastBarrierTimeout = time.NewTimer(c.checkpointTimeout)
					pending := newPendingBarrier(lastBarrier, c.tasksToWaitFor)
					c.logger.Debugf("create pending barrier %d", lastBarrier.CheckpointId)
					c.pendingBarriers[lastBarrier] = pending
					c.lastPendingBarrier = pending
					//let root task send out a barrier
					for _, r := range c.tasksToTrigger {
						go func(life *Task) {
							life.TriggerBarrier(lastBarrier)
						}(r)
					}

				case signal := <-c.barrierSignalChan:
					pb, ok := c.pendingBarriers[signal.Barrier]
					if ok {
						switch signal.Message {
						case ACK:
							c.ack(pb, signal.Name)
						case DEC:
							c.dec(pb, signal.Name)
						}
					} else {
						c.logger.Debugf("receive signal from %s for non existing id %d", signal.Name, signal.CheckpointId)
					}
				case <-lastBarrierTimeout.C:
					pb, ok := c.pendingBarriers[lastBarrier]
					c.logger.Debugf("checkpoint %d timeout", lastBarrier.CheckpointId)
					if ok {
						c.dec(pb, "coordinator")
					} else {
						c.logger.Debugf("timeout checkpoint %d does not exist", lastBarrier.CheckpointId)
					}
				case <-c.done:
					c.logger.Info("coordinator stopped")
					return
				}
			}
		}
	}()
}

func (c *Coordinator) Deactivate(savepoint bool) {
	if status.CAP(&c.status, status.Running, status.Closed) {
		if savepoint {
			c.triggerBarrierChan <- struct{}{}
		} else {
			close(c.done)
		}
	}

}

func (c *Coordinator) Done() <-chan struct{} {
	return c.done
}

func (c *Coordinator) TriggerCheckpoint() {
	c.triggerBarrierChan <- struct{}{}
}

// dec Barrier from pendingBarriers and notify task
func (c *Coordinator) dec(pb *pendingBarrier, name string) {
	c.logger.Debugf("receive dec from %s for id %d", name, pb.CheckpointId)
	pb.dispose()
	c.checkpointFailureNumber += 1
	if c.status.Closed() {
		c.logger.Warn("the coordinator is stopping,but last checkpoint failed,so triggered again")
		c.triggerBarrierChan <- struct{}{}
	}

	if c.checkpointFailureNumber >= c.tolerableCheckpointFailureNumber {
		c.logger.Error("the current number of failed checkpoints has exceeded the maximum tolerable number")
		c.Deactivate(false)
	}
	delete(c.pendingBarriers, pb.Barrier)
	c.notifyCancel(pb.Barrier)

}

func (c *Coordinator) ack(pb *pendingBarrier, name string) {
	c.logger.Debugf("receive ack from %s for id %d", name, pb.CheckpointId)
	if pb.ack(name) && pb.isFullyAck() {
		err := c.storeBackend.Persist(pb.CheckpointId)
		if err != nil {
			c.logger.Errorf("cannot save checkpoint %d due to store error: %v", pb.CheckpointId, err)
			return
		}
		delete(c.pendingBarriers, pb.Barrier)
		//drop the previous pendingBarriers
		for b, _pb := range c.pendingBarriers {
			if _pb.CheckpointId < pb.CheckpointId {
				delete(c.pendingBarriers, b)
			}
		}
		c.notifyComplete(pb.Barrier)
		c.logger.Debugf("totally ack barrier %d", pb.CheckpointId)
	}
	if c.status.Closed() {
		close(c.done)
	}
}

func (c *Coordinator) notifyComplete(barrier Barrier) {
	for _, _task := range c.tasksToWaitFor {
		_task.NotifyBarrierComplete(barrier)
	}
}

func (c *Coordinator) notifyCancel(barrier Barrier) {
	for _, _task := range c.tasksToWaitFor {
		_task.NotifyBarrierCancel(barrier)
	}
}

func NewCoordinator(
	tasksToTrigger []*Task,
	tasksToWaitFor []*Task,
	storeBackend store.Backend,
	barrierSignalChan chan Signal,
	maxConcurrentCheckpoints int,
	minPauseBetweenCheckpoints time.Duration,
	checkpointTimeout time.Duration,
	tolerableCheckpointFailureNumber int,
) *Coordinator {
	return &Coordinator{
		status:             status.Ready,
		done:               make(chan struct{}),
		logger:             log.Global().Named("coordinator"),
		tasksToTrigger:     tasksToTrigger,
		tasksToWaitFor:     tasksToWaitFor,
		pendingBarriers:    map[Barrier]*pendingBarrier{},
		barrierSignalChan:  barrierSignalChan,
		triggerBarrierChan: make(chan struct{}),
		storeBackend:       storeBackend,

		checkpointFailureNumber: 0,

		//convert to millisecond duration
		minPauseBetweenCheckpoints:       int64(minPauseBetweenCheckpoints / time.Millisecond),
		tolerableCheckpointFailureNumber: tolerableCheckpointFailureNumber,
		maxConcurrentCheckpoints:         maxConcurrentCheckpoints,
		checkpointTimeout:                checkpointTimeout,
	}

}
