package main

import (
	"fmt"
	mock_sink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	mock_source "github.com/RuiFG/streaming/streaming-connector/mock-connector/source"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/stream"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
	"os"
	"time"
)

func main() {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("02/Jan/2006:15:04:05 +0800")
	encoderConfig.EncodeLevel = func(level zapcore.Level, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString("[" + level.String() + "]")
	}
	encoderConfig.EncodeName = func(s string, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString("[" + s + "]")
	}
	encoderConfig.ConsoleSeparator = " "
	reporter := prometheus.NewReporter(prometheus.Options{
		Registerer:               prom.DefaultRegisterer,
		Gatherer:                 nil,
		DefaultTimerType:         prometheus.HistogramTimerType,
		DefaultHistogramBuckets:  prometheus.DefaultHistogramBuckets(),
		DefaultSummaryObjectives: prometheus.DefaultSummaryObjectives(),
	})
	env, _ := stream.New("fog", stream.WithPeriodicCheckpoint(5*time.Second),
		stream.WithMetrics(tally.ScopeOptions{
			Tags:           nil,
			Prefix:         "",
			CachedReporter: reporter,
			Separator:      prometheus.DefaultSeparator,
		}, 3*time.Second),
		stream.WithLog(zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig),
			os.Stdout,
			zap.DebugLevel)))
	source, err := mock_source.FormSource[string](env, "mock", func() string {
		return "mock"
	}, 1*time.Second, 1000)
	if err != nil {
		panic(err)
	}
	if err := mock_sink.ToSink[string](source, "sink",
		func(in string) {
			//fmt.Println(in)
		},
		func(timestamp element.Watermark) {
			fmt.Printf("current watermark timestamp %d\n", timestamp)
		}); err != nil {
		panic(err)
	}
	_ = env.Start()

	http.Handle("/metrics", reporter.HTTPHandler())
	fmt.Printf("Serving :8080/metrics\n")
	fmt.Printf("%v\n", http.ListenAndServe(":8080", nil))
	select {}
	env.Stop(false)
}
