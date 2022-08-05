package stream

//func FromSource[OUT any](env *Env, options task2.SourceOptions[OUT]) *SourceStream[OUT] {
//	sourceStream := &SourceStream[OUT]{_env: env, SourceOptions: options}
//	env.AddSourceInit(sourceStream.init)
//	return sourceStream
//
//}
//
//func ToSink[OIN any](stream Stream[OIN], options task2.SinkOptions[OIN]) {
//	outputStream := &SinkStream[OIN]{_env: stream.Env(), SinkOptions: options}
//	stream.AddDownstream(outputStream.Name(), outputStream.init)
//}
