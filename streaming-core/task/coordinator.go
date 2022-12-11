package task

import (
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/store"
	"time"
)

type pendingBarrier struct {
	Barrier
	isDiscarded    bool
	notYetAckTasks map[string]bool
	timeout        *time.Timer
}

func newPendingBarrier(barrier Barrier, tasksToWaitFor []*Task) *pendingBarrier {
	pc := &pendingBarrier{Barrier: barrier}
	nyat := make(map[string]bool)
	for _, _task := range tasksToWaitFor {
		nyat[_task.Name()] = true
	}
	pc.notYetAckTasks = nyat
	//only supports a timeout of more than milliseconds
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
	status          *status.Status
	done            chan struct{}
	logger          log.Logger
	tasksToTrigger  []*Task
	tasksToWaitFor  []*Task
	pendingBarriers map[Barrier]*pendingBarrier

	barrierSignalChan  chan Signal
	triggerBarrierChan chan struct{}
	storeBackend       store.Backend

	checkpointFailureNumber int

	tolerableCheckpointFailureNumber int
	maxConcurrentCheckpoints         int

	minPauseBetweenCheckpoints int64
	checkpointTimeout          time.Duration
}

func (c *Coordinator) Activate() {
	go func() {
		if c.status.CAS(status.Ready, status.Running) {
			c.logger.Infof("started")
			var (
				lastPendingBarrier *pendingBarrier
				timeoutBarrierChan = make(chan Barrier)
			)
			for {
			loop:
				select {
				case <-c.triggerBarrierChan:
					timestamp := time.Now().UnixMilli()
					if c.status.Running() && lastPendingBarrier != nil &&
						timestamp < (lastPendingBarrier.CheckpointId+c.minPauseBetweenCheckpoints) {
						c.logger.Warnf("the time to trigger a checkpoint is less than the min pause time between two checkpoints, cancel checkpoint.")
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
					barrier := Barrier{CheckpointId: timestamp}
					pending := newPendingBarrier(barrier, c.tasksToWaitFor)
					c.logger.Debugf("create pending barrier %d", barrier.CheckpointId)
					pending.timeout = time.AfterFunc(c.checkpointTimeout, func() {
						timeoutBarrierChan <- barrier
					})

					c.pendingBarriers[barrier] = pending
					lastPendingBarrier = pending

					//let root task send out a barrier
					for _, r := range c.tasksToTrigger {
						r.TriggerBarrierAsync(barrier)
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
				case barrier := <-timeoutBarrierChan:
					pb, ok := c.pendingBarriers[barrier]
					c.logger.Debugf("checkpoint %d timeout", pb.CheckpointId)
					if ok {
						c.dec(pb, "coordinator")
					} else {
						c.logger.Debugf("timeout checkpoint %d does not exist", pb.CheckpointId)
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
	if savepoint {
		switch {
		case c.status.CAS(status.Running, status.Closing):
			fallthrough
		case c.status.Closed():
			c.triggerBarrierChan <- struct{}{}
		}
	} else {
		switch {
		case c.status.CAS(status.Running, status.Closed):
			fallthrough
		case c.status.CAS(status.Closing, status.Closed):
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
	delete(c.pendingBarriers, pb.Barrier)
	c.notifyCancel(pb.Barrier)

	if c.checkpointFailureNumber >= c.tolerableCheckpointFailureNumber {
		c.logger.Error("the current number of failed checkpoints has exceeded the maximum tolerable number")
		c.Deactivate(false)
	}
	if c.status.Closing() {
		c.logger.Warn("the coordinator is closing, but last checkpoint failed,so triggered again")
		c.Deactivate(true)
	}
}

func (c *Coordinator) ack(pb *pendingBarrier, name string) {
	c.logger.Debugf("receive ack from %s for id %d", name, pb.CheckpointId)
	if pb.ack(name) &&
		//when is full ack and pb hasn't timed out
		pb.isFullyAck() && pb.timeout.Stop() {
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
		if c.status.Closing() {
			c.Deactivate(false)
		}
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
		status:             status.NewStatus(status.Ready),
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
