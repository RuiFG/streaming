module github.com/RuiFG/streaming/streaming-connector/business-connector

go 1.18

replace github.com/RuiFG/streaming/streaming-core => ./../../streaming-core

require (
	github.com/RuiFG/streaming/streaming-core v0.0.0
	github.com/fsnotify/fsnotify v1.6.0
	github.com/hpcloud/tail v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/xujiajun/mmap-go v1.0.1 // indirect
	github.com/xujiajun/nutsdb v0.11.0 // indirect
	github.com/xujiajun/utils v0.0.0-20220904132955-5f7c5b914235 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/sys v0.0.0-20220908164124-27713097b956 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
)
