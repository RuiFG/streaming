package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/element/metrics"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/uber-go/tally/v4"
	"time"
)

type cachedCount struct {
	fn func(value int64)
}

func (c cachedCount) ReportCount(value int64) {
	c.fn(value)
}

type cachedGauge struct {
	fn func(value float64)
}

func (c cachedGauge) ReportGauge(value float64) {
	c.fn(value)
}

type cachedTimer struct {
	fn func(value time.Duration)
}

func (c cachedTimer) ReportTimer(value time.Duration) {
	c.fn(value)
}

type cachedHistogram struct {
	valueFn    func(bucketLowerBound, bucketUpperBound float64, samples int64)
	durationFn func(bucketLowerBound, bucketUpperBound time.Duration, samples int64)
}

func (h cachedHistogram) ValueBucket(
	bucketLowerBound, bucketUpperBound float64,
) tally.CachedHistogramBucket {
	return cachedHistogramValueBucket{&h, bucketLowerBound, bucketUpperBound}
}

func (h cachedHistogram) DurationBucket(
	bucketLowerBound, bucketUpperBound time.Duration,
) tally.CachedHistogramBucket {
	return cachedHistogramDurationBucket{&h, bucketLowerBound, bucketUpperBound}
}

type cachedHistogramValueBucket struct {
	histogram        *cachedHistogram
	bucketLowerBound float64
	bucketUpperBound float64
}

func (b cachedHistogramValueBucket) ReportSamples(v int64) {
	b.histogram.valueFn(b.bucketLowerBound, b.bucketUpperBound, v)
}

type cachedHistogramDurationBucket struct {
	histogram        *cachedHistogram
	bucketLowerBound time.Duration
	bucketUpperBound time.Duration
}

func (b cachedHistogramDurationBucket) ReportSamples(v int64) {
	b.histogram.durationFn(b.bucketLowerBound, b.bucketUpperBound, v)
}

type metricsSource struct {
	BaseOperator[any, any, metrics.Metric]
	done chan struct{}
}

func (s *metricsSource) Reporting() bool {
	return true
}

func (s *metricsSource) Tagging() bool {
	return true
}

func (s *metricsSource) Capabilities() tally.Capabilities {
	return s
}

func (s *metricsSource) Flush() {}

func (s *metricsSource) ReportCounter(name string, tags map[string]string, value int64) {
	s.Collector.EmitEvent(&element.Event[metrics.Metric]{
		Value: metrics.Metric{
			Type:          metrics.Counter,
			Name:          name,
			Tags:          tags,
			CounterMetric: &metrics.CounterMetric{Value: value},
		},
	})
}

func (s *metricsSource) ReportGauge(name string, tags map[string]string, value float64) {
	s.Collector.EmitEvent(&element.Event[metrics.Metric]{
		Value: metrics.Metric{
			Type:        metrics.Gauge,
			Name:        name,
			Tags:        tags,
			GaugeMetric: &metrics.GaugeMetric{Value: value},
		},
	})
}

func (s *metricsSource) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	s.Collector.EmitEvent(&element.Event[metrics.Metric]{
		Value: metrics.Metric{
			Type:        metrics.Timer,
			Name:        name,
			Tags:        tags,
			TimerMetric: &metrics.TimerMetric{Value: interval},
		},
	})
}

func (s *metricsSource) ReportHistogramValueSamples(name string, tags map[string]string, buckets tally.Buckets, bucketLowerBound, bucketUpperBound float64, samples int64) {
	s.Collector.EmitEvent(&element.Event[metrics.Metric]{
		Value: metrics.Metric{
			Type: metrics.HistogramValue,
			Name: name,
			Tags: tags,
			HistogramValueMetric: &metrics.HistogramValueMetric{
				Bucket:           buckets,
				BucketLowerBound: bucketLowerBound,
				BucketUpperBound: bucketUpperBound,
				Samples:          samples,
			},
		},
	})
}

func (s *metricsSource) ReportHistogramDurationSamples(name string, tags map[string]string, buckets tally.Buckets, bucketLowerBound, bucketUpperBound time.Duration, samples int64) {
	s.Collector.EmitEvent(&element.Event[metrics.Metric]{
		Value: metrics.Metric{
			Type: metrics.HistogramDuration,
			Name: name,
			Tags: tags,
			HistogramDurationMetric: &metrics.HistogramDurationMetric{
				Bucket:           buckets,
				BucketLowerBound: bucketLowerBound,
				BucketUpperBound: bucketUpperBound,
				Samples:          samples,
			},
		},
	})
}

func (s *metricsSource) AllocateCounter(name string, tags map[string]string) tally.CachedCount {
	return cachedCount{func(value int64) {
		s.ReportCounter(name, tags, value)
	}}
}

func (s *metricsSource) AllocateGauge(name string, tags map[string]string) tally.CachedGauge {
	return cachedGauge{func(value float64) {
		s.ReportGauge(name, tags, value)
	}}
}

func (s *metricsSource) AllocateTimer(name string, tags map[string]string) tally.CachedTimer {
	return cachedTimer{func(value time.Duration) {
		s.ReportTimer(name, tags, value)
	}}
}

func (s *metricsSource) AllocateHistogram(name string, tags map[string]string, buckets tally.Buckets) tally.CachedHistogram {
	return cachedHistogram{
		valueFn: func(bucketLowerBound, bucketUpperBound float64, samples int64) {
			s.ReportHistogramValueSamples(name, tags, buckets, bucketLowerBound, bucketUpperBound, samples)
		},
		durationFn: func(bucketLowerBound, bucketUpperBound time.Duration, samples int64) {
			s.ReportHistogramDurationSamples(name, tags, buckets, bucketLowerBound, bucketUpperBound, samples)
		},
	}
}

func (s *metricsSource) Run() {
	<-s.done
}

func (s *metricsSource) Open(ctx Context, collector element.Collector[metrics.Metric]) error {
	if err := s.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	s.done = make(chan struct{})
	return nil
}

func (s *metricsSource) Close() error {
	close(s.done)
	return s.BaseOperator.Close()
}
