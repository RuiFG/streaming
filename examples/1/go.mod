module github.com/RuiFG/streaming/examples/1

go 1.18

replace (
	github.com/RuiFG/streaming/streaming-connector/kafka-connector => ../../streaming-connector/kafka-connector
	github.com/RuiFG/streaming/streaming-connector/mock-connector => ../../streaming-connector/mock-connector
	github.com/RuiFG/streaming/streaming-core => ../../streaming-core
	github.com/RuiFG/streaming/streaming-operator => ../../streaming-operator
)

require (
	github.com/RuiFG/streaming/streaming-connector/mock-connector v0.0.0
	github.com/RuiFG/streaming/streaming-core v0.0.0
	github.com/RuiFG/streaming/streaming-operator v0.0.0
	github.com/pkg/profile v1.6.0
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/xujiajun/mmap-go v1.0.1 // indirect
	github.com/xujiajun/nutsdb v0.11.0 // indirect
	github.com/xujiajun/utils v0.0.0-20220904132955-5f7c5b914235 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
