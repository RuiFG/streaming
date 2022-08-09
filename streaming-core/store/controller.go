package store

import "sync"

type controller struct {
	mm *sync.Map
}

func (c *controller) Range(fn func(key string, state State) bool) {
	c.mm.Range(func(key, value any) bool {
		return fn(key.(string), value.(State))
	})
}

func (c *controller) Load(key string) (State, bool) {
	if load, ok := c.mm.Load(key); !ok {
		return nil, ok
	} else {
		return load.(State), ok
	}

}

func (c *controller) Store(key string, state State) {
	c.mm.Store(key, state)
}

func (c *controller) Delete(key string) {
	c.mm.Delete(key)
}
