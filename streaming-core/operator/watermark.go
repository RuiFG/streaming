package operator

import (
	"bytes"
	"encoding/gob"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/pkg/errors"
	"math"
)

type PartialWatermark struct {
	idle               bool
	watermarkTimestamp int64
}

func (p *PartialWatermark) SetIdle(idle bool) {
	p.idle = idle
}

func (p *PartialWatermark) IsIdle() bool {
	return p.idle
}

func (p *PartialWatermark) GetWatermarkTimestamp() int64 {
	return p.watermarkTimestamp
}

func (p *PartialWatermark) UpdateWatermarkTimestamp(timestamp int64) {
	p.idle = false
	p.watermarkTimestamp = timestamp
}

type CombineWatermark struct {
	Idle                       bool
	CombinedWatermarkTimestamp int64
	PartialWatermarks          []*PartialWatermark
}

func (c *CombineWatermark) IsIdle() bool {
	return c.Idle
}

func (c *CombineWatermark) GetCombinedWatermarkTimestamp() int64 {
	return c.CombinedWatermarkTimestamp
}

func (c *CombineWatermark) UpdateCombinedWatermarkTimestamp() bool {
	var minimumOverAllOutputs int64 = math.MaxInt64
	if len(c.PartialWatermarks) == 0 {
		return false
	}
	allIdle := true
	for _, pw := range c.PartialWatermarks {
		if !pw.IsIdle() {
			minimumOverAllOutputs = int64(math.Min(float64(minimumOverAllOutputs), float64(pw.GetWatermarkTimestamp())))
			allIdle = false
		}
	}
	c.Idle = allIdle
	if !allIdle && minimumOverAllOutputs > c.CombinedWatermarkTimestamp {
		c.CombinedWatermarkTimestamp = minimumOverAllOutputs
		return true
	}
	return false
}

func (c *CombineWatermark) UpdateWatermarkTimestamp(timestamp int64, input int) bool {
	c.PartialWatermarks[input-1].UpdateWatermarkTimestamp(timestamp)
	return c.UpdateCombinedWatermarkTimestamp()
}

func (c *CombineWatermark) UpdateIdle(idle bool, input int) bool {
	c.PartialWatermarks[input-1].SetIdle(idle)
	return c.UpdateCombinedWatermarkTimestamp()
}

func NewCombineWatermark(inputs int) CombineWatermark {
	var partialWatermarks []*PartialWatermark
	for p := 0; p < inputs; p++ {
		partialWatermarks = append(partialWatermarks, &PartialWatermark{true, math.MaxInt64})
	}
	return CombineWatermark{
		Idle:                       true,
		CombinedWatermarkTimestamp: math.MinInt64,
		PartialWatermarks:          partialWatermarks,
	}

}

func NewCombineWatermarkStateDescriptor(key string, count int) store.StateDescriptor[CombineWatermark] {
	return store.StateDescriptor[CombineWatermark]{
		Key: key,
		Initializer: func() CombineWatermark {
			return NewCombineWatermark(count)
		},
		Serializer: func(watermark CombineWatermark) []byte {
			var buffer bytes.Buffer
			decoder := gob.NewEncoder(&buffer)
			if err := decoder.Encode(watermark); err != nil {
				panic(errors.WithMessage(err, "failed to encode combine watermark service to gob bytes"))
			}
			return buffer.Bytes()
		},
		Deserializer: func(byteSlice []byte) CombineWatermark {
			var combineWatermark = CombineWatermark{}
			if err := gob.NewDecoder(bytes.NewReader(byteSlice)).Decode(&combineWatermark); err != nil {
				panic(errors.WithMessage(err, "failed to decode gob bytes"))
			}
			return combineWatermark
		},
	}
}

func init() {
	gob.Register(CombineWatermark{})
}
