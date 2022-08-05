package store

//Warning: key append is unstable

type KeyAppendingState[KEY, NS comparable, IN, OUT any] interface {
}

type keyAppendingState[KEY, NS comparable, IN, OUT any] struct {
	table map[NS]map[KEY][]IN
}

func (k *keyAppendingState[KEY, NS, IN, OUT]) Initialized() bool {
	return true
}

func (k *keyAppendingState[KEY, NS, IN, OUT]) Mirror() MirrorState {
	//TODO implement me
	panic("implement me")
}

func (k *keyAppendingState[KEY, NS, IN, OUT]) Clear() {
	//TODO implement me
	panic("implement me")
}

func (k *keyAppendingState[KEY, NS, IN, OUT]) Get() OUT {

}
