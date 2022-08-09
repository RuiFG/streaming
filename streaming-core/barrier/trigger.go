package barrier

import "github.com/RuiFG/streaming/streaming-core/element"

type Trigger interface {
	TriggerBarrier(detail element.Detail)
}
