package task

import (
	"github.com/RuiFG/streaming/streaming-core/element"
)

type BarrierType uint

const (
	CheckpointBarrier BarrierType = iota
	ExitpointBarrier
)

type Message uint

const (
	ACK Message = iota
	DEC
)

type Signal struct {
	Name string
	Message
	Barrier
}

type Barrier struct {
	Id int64
	BarrierType
}
type InputProcessor[IN1, IN2 any] interface {
	OnElement1(element.Element[IN1])
	OnElement2(element.Element[IN2])
}

type BarrierTrigger interface {
	TriggerBarrier(barrier Barrier)
}

type BarrierListener interface {
	NotifyBarrierComplete(barrier Barrier)
	NotifyBarrierCancel(barrier Barrier)
}

// BarrierAligner for block an input until all barriers are received
type BarrierAligner[IN1, IN2 any] struct {
	BarrierTrigger
	inputProcessor   InputProcessor[IN1, IN2]
	inputCount       int
	currentBarrierId int64
	blockedUpstream  map[string]struct{}
	buffer           []ElementOrBarrier
}

func (h *BarrierAligner[IN1, IN2]) OnElementOrBarrier1(e1 ElementOrBarrier) {
	h.onElementOrBarrier(e1)
}

func (h *BarrierAligner[IN1, IN2]) OnElementOrBarrier2(e2 ElementOrBarrier) {
	h.onElementOrBarrier(e2)
}

func (h *BarrierAligner[IN1, IN2]) onElementOrBarrier(origin ElementOrBarrier) {
	switch e := origin.Value.(type) {
	case Barrier:
		h.processBarrierDetail(e, origin.Upstream)
	case element.Element[IN1]:
		//if blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedUpstream) > 0 {
			if _, ok := h.blockedUpstream[origin.Upstream]; ok {
				h.buffer = append(h.buffer, origin)
				return
			}
		}
		h.inputProcessor.OnElement1(e)
	case element.Element[IN2]:
		//if blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedUpstream) > 0 {
			if _, ok := h.blockedUpstream[origin.Upstream]; ok {
				h.buffer = append(h.buffer, origin)
				return
			}
		}
		h.inputProcessor.OnElement2(e)
	default:
		panic("an impossible error.")
	}
}

func (h *BarrierAligner[IN1, IN2]) processBarrierDetail(barrier Barrier, upstream string) {
	//h.logger.Debugf("aligner process barrier %+v", barrierDetail)
	if h.inputCount == 1 {
		if barrier.Id > h.currentBarrierId {
			h.currentBarrierId = barrier.Id
			h.BarrierTrigger.TriggerBarrier(barrier)
		}
		return
	}
	if len(h.blockedUpstream) > 0 {
		if barrier.Id == h.currentBarrierId {
			h.onUpstream(upstream)
		} else if barrier.Id > h.currentBarrierId {
			//h.logger.Infof("received checkpoint barrier for checkpoint %d before complete current checkpoint %d. skipping current checkpoint.", b.CheckpointId, h.currentBarrierId)

			h.releaseBlocksAndResetBarriers()
			h.beginNewAlignment(barrier, upstream)
		} else {
			return
		}
	} else if barrier.Id > h.currentBarrierId {
		//h.logger.Debugf("aligner process new alignment", b)
		h.beginNewAlignment(barrier, upstream)
	} else {
		return
	}
	if len(h.blockedUpstream) == h.inputCount {
		//h.logger.Debugf("received all barriers, triggering checkpoint %d", b.CheckpointId)
		h.BarrierTrigger.TriggerBarrier(barrier)

		h.releaseBlocksAndResetBarriers()
		// clean up all the buffer
		for _, eAny := range h.buffer {
			h.onElementOrBarrier(eAny)
		}
		h.buffer = make([]ElementOrBarrier, 0)
	}
}

func (h *BarrierAligner[IN1, IN2]) onUpstream(upstream string) {
	if _, ok := h.blockedUpstream[upstream]; !ok {
		h.blockedUpstream[upstream] = struct{}{}
		//h.logger.Debugf("received barrierDetail from channel %s", barrierDetail.Name)
	}
}

func (h *BarrierAligner[IN1, IN2]) releaseBlocksAndResetBarriers() {
	h.blockedUpstream = make(map[string]struct{})
}

func (h *BarrierAligner[IN1, IN2]) beginNewAlignment(barrier Barrier, upstream string) {
	h.currentBarrierId = barrier.Id
	h.onUpstream(upstream)
	//h.logger.Debugf("starting stream alignment for checkpoint %d", barrier.CheckpointId)
}

func NewBarrierAligner[IN1, IN2 any](inputProcessor InputProcessor[IN1, IN2], trigger BarrierTrigger, inputCount int) *BarrierAligner[IN1, IN2] {
	return &BarrierAligner[IN1, IN2]{
		inputProcessor:  inputProcessor,
		BarrierTrigger:  trigger,
		inputCount:      inputCount,
		blockedUpstream: map[string]struct{}{},
	}
}
