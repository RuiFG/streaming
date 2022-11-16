package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"sync"
)

type SinkStreamOptions[IN any] struct {
	Options
	New              operator.NewSink[IN]
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

func ToSink[IN any](upstreams []Stream[IN], sinkOptions SinkStreamOptions[IN]) error {
	var env *Env
	for _, upstream := range upstreams {
		if env == nil {
			env = upstream.Env()
		} else if env != upstream.Env() {
			return ErrMultipleEnv
		}
	}

	sinkStream := &SinkStream[IN]{
		OperatorStream[IN, any, any]{
			options: OperatorStreamOptions[IN, any, any]{
				Options: sinkOptions.Options,
				New: func() operator.Operator[IN, any, any] {
					return &operator.SinkOperatorWrap[IN]{
						Sink: sinkOptions.New(),
					}
				},
				ElementListeners: sinkOptions.ElementListeners,
			},
			env:         env,
			upstreamMap: map[string]struct{}{},
			once:        &sync.Once{},
		},
	}
	for _, upstream := range upstreams {
		upstream.addDownstream(sinkStream.Name(), sinkStream.Init1)
		sinkStream.addUpstream(upstream.Name())
	}
	return nil
}
