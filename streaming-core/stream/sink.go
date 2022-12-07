package stream

import (
	"github.com/RuiFG/streaming/streaming-core/operator"
	"sync"
)

type SinkStreamOptions[IN any] struct {
	Name string
	Sink operator.Sink[IN]
}

type SinkStream[IN any] struct {
	OperatorStream
}

func ToSink[IN any](upstream Stream[IN], sinkOptions SinkStreamOptions[IN]) error {
	sinkStream := &SinkStream[IN]{
		OperatorStream{
			options: OperatorStreamOptions{
				Name: sinkOptions.Name,
				Operator: operator.OneInputOperatorToNormal[IN, any](
					&operator.SinkOperatorWrap[IN]{
						Sink: sinkOptions.Sink,
					}),
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
