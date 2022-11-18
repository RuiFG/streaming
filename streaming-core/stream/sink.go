package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"sync"
)

type SinkStreamOptions[IN any] struct {
	Options
	Sink             operator.Sink[IN]
	ElementListeners []element.Listener[IN, any, any]
}

type WithSinkStreamOptions[IN any] func(options SinkStreamOptions[IN]) SinkStreamOptions[IN]

func ApplyWithSinkStreamOptionsFns[IN any](applyFns []WithSinkStreamOptions[IN]) SinkStreamOptions[IN] {
	options := SinkStreamOptions[IN]{Options: Options{ChannelSize: 1024}}
	for _, fn := range applyFns {
		options = fn(options)
	}
	return options
}

type SinkStream[IN any] struct {
	OperatorStream[IN, any, any]
}

func ToSink[IN any](upstream Stream[IN], sinkOptions SinkStreamOptions[IN]) error {
	sinkStream := &SinkStream[IN]{
		OperatorStream[IN, any, any]{
			options: OperatorStreamOptions[IN, any, any]{
				Options: sinkOptions.Options,
				Operator: operator.OneInputOperatorToNormal[IN, any](
					&operator.SinkOperatorWrap[IN]{
						Sink: sinkOptions.Sink,
					}),
				ElementListeners: sinkOptions.ElementListeners,
			},
			env:         upstream.Env(),
			upstreamMap: map[string]struct{}{},
			once:        &sync.Once{},
		},
	}
	upstream.addDownstream(sinkStream.Name(), sinkStream.Init(0))
	sinkStream.addUpstream(upstream.Name())
	return nil
}
