package plugins

import (
	"3/pkg/format"
	"github.com/golang/protobuf/proto"
)

type aggregator[T proto.Message] struct {
	Plugin[T]
}

func (a *aggregator[T]) GetResult(acc map[string]T) *Output {
	message := a.Plugin.ToMessage(acc)
	bytes, err := proto.Marshal(message)
	if err != nil {
		return nil
	}
	return &Output{
		Plugin: a.PluginName(),
		Buffer: bytes,
	}
}

func (a *aggregator[T]) Add(acc map[string]T, in *format.Log) map[string]T {
	if acc == nil {
		acc = map[string]T{}
	}
	if a.Plugin.NeedCalculate(in) {
		id := a.Plugin.ID(in)
		v, ok := acc[id]
		if !ok {
			acc[id] = a.Plugin.NewStruct(in)
			v = acc[id]
		}
		a.Plugin.Calculate(in, v)
	}
	return acc
}
