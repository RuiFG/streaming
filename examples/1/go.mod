module github.com/RuiFG/streaming/examples/1

go 1.18

replace (
	github.com/RuiFG/streaming/streaming-connector/kafka-connector => ../../streaming-connector/kafka-connector
	github.com/RuiFG/streaming/streaming-connector/mock-connector => ../../streaming-connector/mock-connector
	github.com/RuiFG/streaming/streaming-core => ../../streaming-core
	github.com/RuiFG/streaming/streaming-operator => ../../streaming-operator
)

require (
	github.com/RuiFG/streaming/streaming-connector/kafka-connector v0.0.0
	github.com/RuiFG/streaming/streaming-connector/mock-connector v0.0.0
	github.com/RuiFG/streaming/streaming-core v0.0.0
	github.com/RuiFG/streaming/streaming-operator v0.0.0
	github.com/Shopify/sarama v1.35.0
	github.com/pkg/profile v1.6.0
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.15.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/xujiajun/mmap-go v1.0.1 // indirect
	github.com/xujiajun/nutsdb v0.9.0 // indirect
	github.com/xujiajun/utils v0.0.0-20190123093513-8bf096c4f53b // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/net v0.0.0-20220708220712-1185a9018129 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
)
