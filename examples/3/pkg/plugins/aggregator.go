package plugins

import (
	"3/pkg/format"
	"github.com/golang/protobuf/proto"
)

type aggregator[T proto.Message] struct {
	plugin Plugin[T]
}

func (a *aggregator[T]) Add(acc map[string]T, in *format.Log) map[string]T {
	if acc == nil {
		acc = map[string]T{}
	}
	if a.plugin.NeedCalculate(in) {
		id := a.plugin.ID(in)
		v, ok := acc[id]
		if !ok {
			acc[id] = a.plugin.NewStruct(in)
			v = acc[id]
		}
		a.plugin.Calculate(in, v)
	}
	return acc
}

func (a *aggregator[T]) GetResult(acc map[string]T) []proto.Message {
	result := make([]proto.Message, len(acc))
	index := 0
	for _, v := range acc {
		result[index] = v
		index++
	}
	return result
}
