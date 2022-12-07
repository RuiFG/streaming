package operator

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombineWatermarkInit(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	assert.True(t, combineWatermark.IsIdle())
}

func TestCombineWatermarkTwoInputUpdateWatermark(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	var currentTimestamp element.Watermark = 1
	assert.True(t, combineWatermark.UpdateWatermark(currentTimestamp+1, 1))
	assert.False(t, combineWatermark.IsIdle())
	assert.False(t, combineWatermark.UpdateWatermark(currentTimestamp, 2))
	assert.False(t, combineWatermark.UpdateWatermark(currentTimestamp+2, 1))
	assert.True(t, combineWatermark.UpdateWatermark(currentTimestamp+2, 2))
}

func TestCombineWatermarkOneInputUpdateWatermark(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	for i := 0; i < 100; i++ {
		assert.True(t, combineWatermark.UpdateWatermark(element.Watermark(i), 1))
	}
	assert.Equal(t, element.Watermark(99), combineWatermark.CombinedWatermark)

}

func TestNewCombineWatermarkUpdateIdle(t *testing.T) {
	t.Run("case-1", func(t *testing.T) {
		combineWatermark := NewCombineWatermark(2)
		combineWatermark.UpdateWatermark(1, 1)
		combineWatermark.UpdateWatermark(100, 2)
		assert.Equal(t, element.Watermark(1), combineWatermark.CombinedWatermark)
		assert.True(t, combineWatermark.UpdateIdle(true, 1))
		assert.Equal(t, element.Watermark(100), combineWatermark.CombinedWatermark)
	})
	t.Run("case-2", func(t *testing.T) {
		combineWatermark := NewCombineWatermark(2)
		combineWatermark.UpdateWatermark(100, 1)
		combineWatermark.UpdateWatermark(1, 2)
		assert.Equal(t, element.Watermark(100), combineWatermark.CombinedWatermark)
		assert.False(t, combineWatermark.UpdateIdle(true, 1))
	})
}
