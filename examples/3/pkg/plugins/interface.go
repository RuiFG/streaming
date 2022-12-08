package plugins

import (
	"3/pkg/format"
	"github.com/golang/protobuf/proto"
)

type Plugin[T proto.Message] interface {
	NeedCalculate(log *format.Log) bool
	ID(log *format.Log) string
	NewStruct(log *format.Log) T
	Calculate(log *format.Log, v T)
	PluginName() string
}
