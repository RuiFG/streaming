package metrics

import (
	"github.com/uber-go/tally/v4"
	"time"
)

type Type uint32

const (
	Counter Type = iota
	Gauge
	Timer
	HistogramValue
	HistogramDuration
)

type Metric struct {
	Type
	Name string
	Tags map[string]string
	*CounterMetric
	*GaugeMetric
	*TimerMetric
	*HistogramValueMetric
	*HistogramDurationMetric
}

type CounterMetric struct {
	Value int64
}

type GaugeMetric struct {
	Value float64
}

type TimerMetric struct {
	Value time.Duration
}

type HistogramValueMetric struct {
	Bucket           tally.Buckets
	BucketLowerBound float64
	BucketUpperBound float64
	Samples          int64
}

type HistogramDurationMetric struct {
	Bucket           tally.Buckets
	BucketLowerBound time.Duration
	BucketUpperBound time.Duration
	Samples          int64
}
