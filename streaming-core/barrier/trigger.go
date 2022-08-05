package barrier

type Trigger interface {
	TriggerBarrier(detail Detail)
}
