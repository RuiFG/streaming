package operator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombineWatermarkInit(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	assert.True(t, combineWatermark.IsIdle())
}

func TestCombineWatermarkTwoInputUpdateWatermark(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	var currentTimestamp int64 = 1
	assert.True(t, combineWatermark.UpdateWatermarkTimestamp(currentTimestamp+1, 1))
	assert.False(t, combineWatermark.IsIdle())
	assert.False(t, combineWatermark.UpdateWatermarkTimestamp(currentTimestamp, 2))
	assert.False(t, combineWatermark.UpdateWatermarkTimestamp(currentTimestamp+2, 1))
	assert.True(t, combineWatermark.UpdateWatermarkTimestamp(currentTimestamp+2, 2))
}

func TestCombineWatermarkOneInputUpdateWatermark(t *testing.T) {
	combineWatermark := NewCombineWatermark(2)
	for i := 0; i < 100; i++ {
		assert.True(t, combineWatermark.UpdateWatermarkTimestamp(int64(i), 1))
	}
	assert.Equal(t, int64(99), combineWatermark.CombinedWatermarkTimestamp)

}

func TestNewCombineWatermarkUpdateIdle(t *testing.T) {
	t.Run("case-1", func(t *testing.T) {
		combineWatermark := NewCombineWatermark(2)
		combineWatermark.UpdateWatermarkTimestamp(1, 1)
		combineWatermark.UpdateWatermarkTimestamp(100, 2)
		assert.Equal(t, int64(1), combineWatermark.CombinedWatermarkTimestamp)
		assert.True(t, combineWatermark.UpdateIdle(true, 1))
		assert.Equal(t, int64(100), combineWatermark.CombinedWatermarkTimestamp)
	})
	t.Run("case-2", func(t *testing.T) {
		combineWatermark := NewCombineWatermark(2)
		combineWatermark.UpdateWatermarkTimestamp(100, 1)
		combineWatermark.UpdateWatermarkTimestamp(1, 2)
		assert.Equal(t, int64(100), combineWatermark.CombinedWatermarkTimestamp)
		assert.False(t, combineWatermark.UpdateIdle(true, 1))
	})
}
