package task

import (
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
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
	logger          *zap.Logger
	scope           tally.Scope
	tasksToTrigger  []*Task
	tasksToWaitFor  []*Task
	pendingBarriers map[Barrier]*pendingBarrier

	barrierSignalChan  <-chan Signal
	triggerBarrierChan chan struct{}
	storeBackend       store.Backend

	checkpointFailureNumber int

	tolerableCheckpointFailureNumber int
	maxConcurrentCheckpoints         int

	minPauseBetweenCheckpoints int64
	checkpointTimeout          time.Duration

	//metrics
	totalCheckpointCounter     tally.Counter
	currentCheckpointGauge     tally.Gauge
	pendingCheckpointGauge     tally.Gauge
	succeededCheckpointCounter tally.Counter
	failedCheckpointCounter    tally.Counter
}

func (c *Coordinator) Activate() {
	go func() {
		if c.status.CAS(status.Ready, status.Running) {
			c.logger.Info("started")
			var (
				lastPendingBarrier *pendingBarrier
				timeoutBarrierChan = make(chan Barrier)
			)
			for {
			loop:
				c.pendingCheckpointGauge.Update(float64(len(c.pendingBarriers)))
				select {
				case <-c.triggerBarrierChan:
					timestamp := time.Now().UnixMilli()
					if c.status.Running() && lastPendingBarrier != nil &&
						timestamp < (lastPendingBarrier.CheckpointId+c.minPauseBetweenCheckpoints) {
						c.logger.Warn("the time to trigger a checkpoint is less than the min pause time between two checkpoints, cancel checkpoint.")
						goto loop
					}

					if len(c.pendingBarriers) > c.maxConcurrentCheckpoints {
						c.logger.Warn("waiting barriers exceeds the maximum number, cancel checkpoint.",
							zap.Int("maxConcurrentCheckpoints", c.maxConcurrentCheckpoints))
						goto loop
					}

					for _, _task := range c.tasksToWaitFor {
						if !_task.Running() {
							c.logger.Warn("task is not running, cancel checkpoint.",
								zap.String("task", _task.Name()))
							goto loop
						}
					}
					barrier := Barrier{CheckpointId: timestamp}
					pending := newPendingBarrier(barrier, c.tasksToWaitFor)
					c.logger.Debug("create pending barrier.", zap.Int64("id", barrier.CheckpointId))
					pending.timeout = time.AfterFunc(c.checkpointTimeout, func() {
						timeoutBarrierChan <- barrier
					})

					c.pendingBarriers[barrier] = pending
					lastPendingBarrier = pending
					c.currentCheckpointGauge.Update(float64(barrier.CheckpointId))
					c.totalCheckpointCounter.Inc(1)
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
						c.logger.Debug("receive signal for non existing id.",
							zap.String("operator", signal.Name), zap.Int64("id", signal.CheckpointId))
					}
				case barrier := <-timeoutBarrierChan:
					pb, ok := c.pendingBarriers[barrier]
					c.logger.Debug("checkpoint timeout.", zap.Int64("id", pb.CheckpointId))
					if ok {
						c.dec(pb, "coordinator")
					} else {
						c.logger.Debug("timeout checkpoint does not exist.", zap.Int64("id", pb.CheckpointId))
					}
				case <-c.done:
					c.logger.Info("coordinator stopped.")
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
	c.logger.Debug("receive dec for checkpoint.", zap.String("operator", name), zap.Int64("id", pb.CheckpointId))
	pb.dispose()
	c.checkpointFailureNumber += 1
	delete(c.pendingBarriers, pb.Barrier)
	c.notifyCancel(pb.Barrier)

	if c.checkpointFailureNumber >= c.tolerableCheckpointFailureNumber {
		c.logger.Error("the current number of failed checkpoints has exceeded the maximum tolerable number.")
		c.Deactivate(false)
	}
	if c.status.Closing() {
		c.logger.Warn("the coordinator is closing, but last checkpoint failed,so triggered again.")
		c.Deactivate(true)
	}
}

func (c *Coordinator) ack(pb *pendingBarrier, name string) {
	c.logger.Debug("receive ack for checkpoint.", zap.String("operator", name), zap.Int64("id", pb.CheckpointId))
	if pb.ack(name) &&
		//when is full ack and pb hasn't timed out
		pb.isFullyAck() && pb.timeout.Stop() {
		err := c.storeBackend.Persist(pb.CheckpointId)
		if err != nil {
			c.logger.Error("can't save checkpoint due to store error", zap.Int64("id", pb.CheckpointId), zap.Error(err))
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
		c.logger.Debug("totally ack barrier.", zap.Int64("id", pb.CheckpointId))
		if c.status.Closing() {
			c.Deactivate(false)
		}
	}

}

func (c *Coordinator) notifyComplete(barrier Barrier) {
	c.succeededCheckpointCounter.Inc(1)
	for _, _task := range c.tasksToWaitFor {
		_task.NotifyBarrierComplete(barrier)
	}
}

func (c *Coordinator) notifyCancel(barrier Barrier) {
	c.failedCheckpointCounter.Inc(1)
	for _, _task := range c.tasksToWaitFor {
		_task.NotifyBarrierCancel(barrier)
	}
}

func NewCoordinator(
	logger *zap.Logger,
	scope tally.Scope,
	tasksToTrigger []*Task,
	tasksToWaitFor []*Task,
	storeBackend store.Backend,
	barrierSignalChan <-chan Signal,
	maxConcurrentCheckpoints int,
	minPauseBetweenCheckpoints time.Duration,
	checkpointTimeout time.Duration,
	tolerableCheckpointFailureNumber int,
) *Coordinator {
	return &Coordinator{
		status:                           status.NewStatus(status.Ready),
		done:                             make(chan struct{}),
		logger:                           logger,
		scope:                            scope,
		tasksToTrigger:                   tasksToTrigger,
		tasksToWaitFor:                   tasksToWaitFor,
		pendingBarriers:                  map[Barrier]*pendingBarrier{},
		barrierSignalChan:                barrierSignalChan,
		triggerBarrierChan:               make(chan struct{}),
		storeBackend:                     storeBackend,
		checkpointFailureNumber:          0,
		tolerableCheckpointFailureNumber: tolerableCheckpointFailureNumber,
		maxConcurrentCheckpoints:         maxConcurrentCheckpoints,
		//convert to millisecond duration
		minPauseBetweenCheckpoints: int64(minPauseBetweenCheckpoints / time.Millisecond),
		checkpointTimeout:          checkpointTimeout,

		totalCheckpointCounter:     scope.Counter("total_checkpoint"),
		currentCheckpointGauge:     scope.Gauge("current_checkpoint"),
		pendingCheckpointGauge:     scope.Gauge("pending_checkpoint"),
		succeededCheckpointCounter: scope.Counter("succeeded_checkpoint"),
		failedCheckpointCounter:    scope.Counter("failed_checkpoint"),
	}

}
