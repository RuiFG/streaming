# streaming

go-streaming is a framework that combines stream processing and stream computing capabilities

## Components

There are three basic components of a construction stream application.

### Source

Source is the end point of the data (etc. Http Kafka...) and needs to implement `component.Source` interface.

### Operator(OneInput and TwoInput)

Operator is divided into `OneInputOperator` and `TwoInputOperator`,
`OneInputOperator` to process single-stream data, `TwoInputOperator` is used to merge multiple single streams to achieve
the purpose of aggregation processing. Their interfaces are all `component.Operator`, which can be distinguished by
whether to implement `processEvent1` and `processEvent2` func.

### Sink

Sink is the end point of the data, which can be sent to other components or output at the terminal (as long as you want)
and needs to implement `component.Sink` interface.

## elements

* Event
* Watermark
* Barrier

.........

## RoadMap

- [ ] code comment
- [ ] store package
- [ ] log package
- [ ] dag validate
- [ ] streaming-connector kafka-connector kafka-source
- [ ] streaming-connector kafka-connector file-sink
- [ ] streaming-connector http-connector http-sink
- [ ] metric package
- 

