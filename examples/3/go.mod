module 3

go 1.19

replace (
	github.com/RuiFG/streaming/streaming-connector/business-connector => ../../streaming-connector/business-connector
	github.com/RuiFG/streaming/streaming-connector/mock-connector => ../../streaming-connector/mock-connector
	github.com/RuiFG/streaming/streaming-core => ../../streaming-core
	github.com/RuiFG/streaming/streaming-operator => ../../streaming-operator

)

require (
	bt.baishancloud.com/log/bsip v1.3.1
	github.com/RuiFG/streaming/streaming-connector/business-connector v0.0.0
	github.com/RuiFG/streaming/streaming-connector/mock-connector v0.0.0-00010101000000-000000000000
	github.com/RuiFG/streaming/streaming-core v0.0.0
	github.com/RuiFG/streaming/streaming-operator v0.0.0-00010101000000-000000000000
	github.com/aws/aws-sdk-go v1.44.155
	github.com/golang/protobuf v1.5.2
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.2.0
	github.com/spf13/cobra v1.6.1
	github.com/spf13/viper v1.14.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/ipipdotnet/ipdb-go v1.3.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.1 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.5 // indirect
	github.com/spf13/afero v1.9.2 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.4.1 // indirect
	github.com/xujiajun/mmap-go v1.0.1 // indirect
	github.com/xujiajun/nutsdb v0.11.0 // indirect
	github.com/xujiajun/utils v0.0.0-20220904132955-5f7c5b914235 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/sys v0.1.0 // indirect
	golang.org/x/term v0.1.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
