module github.com/RuiFG/streaming/examples/mock

go 1.18

replace (
	github.com/RuiFG/streaming/streaming-connector => ../../streaming-connector
	github.com/RuiFG/streaming/streaming-core => ../../streaming-core
)

require (
	github.com/RuiFG/streaming/streaming-connector v0.0.0
	github.com/RuiFG/streaming/streaming-core v0.0.1
	github.com/prometheus/client_golang v1.11.0
	github.com/uber-go/tally/v4 v4.1.4
	go.uber.org/zap v1.24.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/twmb/murmur3 v1.1.5 // indirect
	github.com/xujiajun/mmap-go v1.0.1 // indirect
	github.com/xujiajun/nutsdb v0.11.0 // indirect
	github.com/xujiajun/utils v0.0.0-20220904132955-5f7c5b914235 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)
