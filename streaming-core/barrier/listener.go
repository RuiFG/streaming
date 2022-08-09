package barrier

import "github.com/RuiFG/streaming/streaming-core/element"

type Listener interface {
	NotifyBarrierCome(detail element.Detail)
	NotifyBarrierComplete(detail element.Detail)
	NotifyBarrierCancel(detail element.Detail)
}
