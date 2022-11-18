package task

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

type BarrierTrigger interface {
	TriggerBarrier(barrier Barrier)
}

type BarrierListener interface {
	NotifyBarrierComplete(barrier Barrier)
	NotifyBarrierCancel(barrier Barrier)
}

type dataProcessor interface {
	ProcessData(data internalData)
}

// BarrierAligner for block an input until all barriers are received
type BarrierAligner struct {
	BarrierTrigger
	processor        dataProcessor
	inputCount       int
	currentBarrierId int64
	blockedIndexes   map[int]struct{}
	buffer           []internalData
}

func (h *BarrierAligner) Handler(data internalData) {
	if barrier, ok := data.eob.(Barrier); ok {
		h.processBarrierDetail(barrier, data.index)
	} else {
		//if blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedIndexes) > 0 {
			if _, ok := h.blockedIndexes[data.index]; ok {
				h.buffer = append(h.buffer, data)
				return
			}
		}
		h.processor.ProcessData(data)
	}
}

func (h *BarrierAligner) processBarrierDetail(barrier Barrier, index int) {
	//h.logger.Debugf("aligner process barrier %+v", barrierDetail)
	if h.inputCount == 1 {
		if barrier.Id > h.currentBarrierId {
			h.currentBarrierId = barrier.Id
			h.BarrierTrigger.TriggerBarrier(barrier)
		}
		return
	}
	if len(h.blockedIndexes) > 0 {
		if barrier.Id == h.currentBarrierId {
			h.onUpstream(index)
		} else if barrier.Id > h.currentBarrierId {
			//h.logger.Infof("received checkpoint barrier for checkpoint %d before complete current checkpoint %d. skipping current checkpoint.", b.CheckpointId, h.currentBarrierId)

			h.releaseBlocksAndResetBarriers()
			h.beginNewAlignment(barrier, index)
		} else {
			return
		}
	} else if barrier.Id > h.currentBarrierId {
		//h.logger.Debugf("aligner process new alignment", b)
		h.beginNewAlignment(barrier, index)
	} else {
		return
	}
	if len(h.blockedIndexes) == h.inputCount {
		//h.logger.Debugf("received all barriers, triggering checkpoint %d", b.CheckpointId)
		h.BarrierTrigger.TriggerBarrier(barrier)

		h.releaseBlocksAndResetBarriers()
		// clean up all the buffer
		for _, eAny := range h.buffer {
			h.Handler(eAny)
		}
		h.buffer = make([]internalData, 0)
	}
}

func (h *BarrierAligner) onUpstream(index int) {
	if _, ok := h.blockedIndexes[index]; !ok {
		h.blockedIndexes[index] = struct{}{}
		//h.logger.Debugf("received barrierDetail from channel %s", barrierDetail.Name)
	}
}

func (h *BarrierAligner) releaseBlocksAndResetBarriers() {
	h.blockedIndexes = make(map[int]struct{})
}

func (h *BarrierAligner) beginNewAlignment(barrier Barrier, index int) {
	h.currentBarrierId = barrier.Id
	h.onUpstream(index)
	//h.logger.Debugf("starting stream alignment for checkpoint %d", barrier.CheckpointId)
}

func NewBarrierAligner(processor dataProcessor, trigger BarrierTrigger, inputCount int) *BarrierAligner {
	return &BarrierAligner{
		processor:      processor,
		BarrierTrigger: trigger,
		inputCount:     inputCount,
		blockedIndexes: map[int]struct{}{},
	}
}
