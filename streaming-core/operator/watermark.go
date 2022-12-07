package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"math"
)

type PartialWatermark struct {
	idle      bool
	watermark element.Watermark
}

func (p *PartialWatermark) SetIdle(idle bool) {
	p.idle = idle
}

func (p *PartialWatermark) IsIdle() bool {
	return p.idle
}

func (p *PartialWatermark) GetWatermark() element.Watermark {
	return p.watermark
}

func (p *PartialWatermark) UpdateWatermark(watermark element.Watermark) {
	p.idle = false
	p.watermark = watermark
}

type CombineWatermark struct {
	Idle              bool
	CombinedWatermark element.Watermark
	PartialWatermarks []*PartialWatermark
}

func (c *CombineWatermark) IsIdle() bool {
	return c.Idle
}

func (c *CombineWatermark) GetCombinedWatermark() element.Watermark {
	return c.CombinedWatermark
}

func (c *CombineWatermark) UpdateCombinedWatermark() bool {
	var minimumOverAllOutputs element.Watermark = math.MaxInt64
	if len(c.PartialWatermarks) == 0 {
		return false
	}
	allIdle := true
	for _, pw := range c.PartialWatermarks {
		if !pw.IsIdle() {
			minimumOverAllOutputs = element.Watermark(math.Min(float64(minimumOverAllOutputs), float64(pw.GetWatermark())))
			allIdle = false
		}
	}
	c.Idle = allIdle
	if !allIdle && minimumOverAllOutputs > c.CombinedWatermark {
		c.CombinedWatermark = minimumOverAllOutputs
		return true
	}
	return false
}

func (c *CombineWatermark) UpdateWatermark(watermark element.Watermark, input int) bool {
	c.PartialWatermarks[input-1].UpdateWatermark(watermark)
	return c.UpdateCombinedWatermark()
}

func (c *CombineWatermark) UpdateIdle(idle bool, input int) bool {
	c.PartialWatermarks[input-1].SetIdle(idle)
	return c.UpdateCombinedWatermark()
}

func NewCombineWatermark(inputs int) *CombineWatermark {
	var partialWatermarks []*PartialWatermark
	for p := 0; p < inputs; p++ {
		partialWatermarks = append(partialWatermarks, &PartialWatermark{true, math.MaxInt64})
	}
	return &CombineWatermark{
		Idle:              true,
		CombinedWatermark: math.MinInt64,
		PartialWatermarks: partialWatermarks,
	}

}
