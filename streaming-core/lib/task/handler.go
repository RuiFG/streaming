package task

import (
	barrier2 "streaming/streaming-core/lib/barrier"
	"streaming/streaming-core/lib/element"
)

type ElementHandler[IN1, IN2 any] interface {
	OnElement1(e1 element.Element[IN1])
	OnElement2(e2 element.Element[IN2])
}

type TriggerHandlerGeneratorFn[IN1, IN2 any] func(chan<- barrier2.Type) ElementHandler[IN1, IN2]

//TrackerHandler for at least once, simple track barriers
type TrackerHandler[IN1, IN2 any] struct {
	handlers []ElementHandler[IN1, IN2]
	barrier2.Trigger
	inputCount     int
	pendingBarrier map[barrier2.Detail]int
}

func (h *TrackerHandler[IN1, IN2]) OnElement1(e1 element.Element[IN1]) {
	if e1.Type() == element.BarrierElement {
		h.processBarrierDetail(e1.AsBarrier().Detail)
	} else {
		for _, handler := range h.handlers {
			handler.OnElement1(e1)
		}

	}
}

func (h *TrackerHandler[IN1, IN2]) OnElement2(e2 element.Element[IN2]) {
	if e2.Type() == element.BarrierElement {
		h.processBarrierDetail(e2.AsBarrier().Detail)
	} else {
		for _, handler := range h.handlers {
			handler.OnElement2(e2)
		}
	}
}

func (h *TrackerHandler[IN1, IN2]) processBarrierDetail(barrierDetail barrier2.Detail) {
	if h.inputCount == 1 {
		h.Trigger.TriggerBarrier(barrierDetail)
		return
	}
	if c, ok := h.pendingBarrier[barrierDetail]; ok {
		c += 1
		if c == h.inputCount {
			h.Trigger.TriggerBarrier(barrierDetail)
			delete(h.pendingBarrier, barrierDetail)
			for cBarrierDetail := range h.pendingBarrier {
				if cBarrierDetail.Id < barrierDetail.Id {
					delete(h.pendingBarrier, cBarrierDetail)
				}
			}
		} else {
			h.pendingBarrier[barrierDetail] = c
		}
	} else {
		h.pendingBarrier[barrierDetail] = 1
	}
}

//AlignerHandler for at exact once, block an input until all barriers are received
type AlignerHandler[IN1, IN2 any] struct {
	handlers []ElementHandler[IN1, IN2]
	barrier2.Trigger
	inputCount       int
	currentBarrierId int64
	blockedUpstream  map[string]bool
	buffer           []any
}

func (h *AlignerHandler[IN1, IN2]) OnElement1(e1 element.Element[IN1]) {
	if e1.Type() == element.BarrierElement {
		h.processBarrierDetail(e1.AsBarrier().Detail, e1.AsBarrier().Upstream)
		return
	} else {
		//if blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedUpstream) > 0 {
			if _, ok := h.blockedUpstream[e1.GetMeta().Upstream]; ok {
				h.buffer = append(h.buffer, e1)
				return
			}
		}
	}
	for _, handler := range h.handlers {
		handler.OnElement1(e1)
	}
}

func (h *AlignerHandler[IN1, IN2]) OnElement2(e2 element.Element[IN2]) {
	if e2.Type() == element.BarrierElement {
		h.processBarrierDetail(e2.AsBarrier().Detail, e2.AsBarrier().Upstream)
		return
	} else {
		//if blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedUpstream) > 0 {
			if _, ok := h.blockedUpstream[e2.GetMeta().Upstream]; ok {
				h.buffer = append(h.buffer, e2)
				return
			}
		}
	}
	for _, handler := range h.handlers {
		handler.OnElement2(e2)
	}
}

func (h *AlignerHandler[IN1, IN2]) processBarrierDetail(barrierDetail barrier2.Detail, upstream string) {
	//h.logger.Debugf("aligner process barrier %+v", barrierDetail)
	if h.inputCount == 1 {
		if barrierDetail.Id > h.currentBarrierId {
			h.currentBarrierId = barrierDetail.Id
			h.Trigger.TriggerBarrier(barrierDetail)
		}
		return
	}
	if len(h.blockedUpstream) > 0 {
		if barrierDetail.Id == h.currentBarrierId {
			h.onUpstream(upstream)
		} else if barrierDetail.Id > h.currentBarrierId {
			//h.logger.Infof("received checkpoint barrier for checkpoint %d before complete current checkpoint %d. skipping current checkpoint.", b.CheckpointId, h.currentBarrierId)

			h.releaseBlocksAndResetBarriers()
			h.beginNewAlignment(barrierDetail, upstream)
		} else {
			return
		}
	} else if barrierDetail.Id > h.currentBarrierId {
		//h.logger.Debugf("aligner process new alignment", b)
		h.beginNewAlignment(barrierDetail, upstream)
	} else {
		return
	}
	if len(h.blockedUpstream) == h.inputCount {
		//h.logger.Debugf("received all barriers, triggering checkpoint %d", b.CheckpointId)
		h.Trigger.TriggerBarrier(barrierDetail)

		h.releaseBlocksAndResetBarriers()
		// clean up all the buffer
		for _, eAny := range h.buffer {
			switch e := eAny.(type) {
			case element.Element[IN1]:
				h.OnElement1(e)
			case element.Element[IN2]:
				h.OnElement2(e)
			}
		}
		h.buffer = make([]any, 0)
	}
}

func (h *AlignerHandler[IN1, IN2]) onUpstream(upstream string) {

	if _, ok := h.blockedUpstream[upstream]; !ok {
		h.blockedUpstream[upstream] = true
		//h.logger.Debugf("received barrierDetail from channel %s", barrierDetail.Name)
	}
}

func (h *AlignerHandler[IN1, IN2]) releaseBlocksAndResetBarriers() {
	h.blockedUpstream = make(map[string]bool)
}

func (h *AlignerHandler[IN1, IN2]) beginNewAlignment(barrierDetail barrier2.Detail, upstream string) {
	h.currentBarrierId = barrierDetail.Id
	h.onUpstream(upstream)
	//h.logger.Debugf("starting stream alignment for checkpoint %d", barrier.CheckpointId)
}
