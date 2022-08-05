package barrier

type Listener interface {
	NotifyComplete(detail Detail)
	NotifyCancel(detail Detail)
}
