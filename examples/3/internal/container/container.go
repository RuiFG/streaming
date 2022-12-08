package main

import (
	"3/internal/container/config"
	"3/pkg/format"
	"3/pkg/plugins"
	"3/pkg/plugins/pb"
	"fmt"
	"github.com/RuiFG/streaming/streaming-connector/business-connector/source/geddon"
	mock_sink "github.com/RuiFG/streaming/streaming-connector/mock-connector/sink"
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/RuiFG/streaming/streaming-core/stream"
	flat_map_operator "github.com/RuiFG/streaming/streaming-operator/flat_map"
	"github.com/RuiFG/streaming/streaming-operator/window"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

func Start(cmd *cobra.Command, args []string) {
	application := config.Get()
	level := log.InfoLevel
	if application.Debug == true {
		level = log.DebugLevel
	}
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder).WithLevel(level))
	option := stream.DefaultEnvironmentOptions
	option.EnablePeriodicCheckpoint = 60 * time.Second
	option.CheckpointsDir = application.CheckpointsDir
	env, err := stream.New(option)
	if err != nil {
		log.Global().Fatalf("can't new environment", "err", err)
	}
	if application.Geddon == nil {
		log.Global().Fatalf("geddon config can't be nil")
	}
	if application.Format == nil {
		log.Global().Fatalf("format config can't be nil")
	}
	if err = format.Init(application.Format.IPDB, application.Format.IPV6DB, application.Format.ASNDB,
		application.Format.ViewDB.Endpoint, application.Format.ViewDB.Bucket, application.Format.ViewDB.Prefix, application.Format.ViewDB.Path, application.Format.ViewDB.Delay); err != nil {
		log.Global().Fatalw("failed to init format module", "err", err)
	}
	source, err := geddon.FromSource(env, "geddon",
		geddon.WithDir[string](application.Geddon.Dir, regexp.MustCompile(application.Geddon.Pattern)),
		geddon.WithFormat[string](func(filename string, data []byte) string {
			return strings.TrimSpace(*(*string)(unsafe.Pointer(&data)))
		}, '\n'),
		geddon.WithPeriodicScan[string](application.Geddon.ScanDuration))
	if err != nil {
		log.Global().Error("can't new geddon source", "err", err)
	}
	formatStream, err := flat_map_operator.Apply(source, "format-log",
		flat_map_operator.WithFn(func(raw string) []*format.Log {
			formatLog, formatErr := format.Format(raw)
			if formatErr != nil {
				return nil
			}
			return []*format.Log{formatLog}
		}))
	if err != nil {
		panic(err)
	}
	aggregator, _ := window.Apply(formatStream, "asd",
		window.WithNonKeySelector[*format.Log, map[string]*pb.Region, []proto.Message, []proto.Message](),
		window.WithTumblingProcessingTime[struct{}, *format.Log, map[string]*pb.Region, []proto.Message, []proto.Message](60*time.Second, 0),
		window.WithAggregator[struct{}, *format.Log, map[string]*pb.Region, []proto.Message](plugins.RegionAggregator()))
	if err := mock_sink.ToSink(aggregator, "mock",
		func(in []proto.Message) {
			fmt.Println(in)
		},
		func(timestamp element.Watermark) {
			fmt.Printf("current watermark timestamp %d\n", timestamp)
		}); err != nil {
		panic(err)
	}
	_ = env.Start()
	<-env.Done()
	time.Sleep(5 * time.Second)
	env.Stop(true)
}

func main() {
	Start(nil, nil)
}
