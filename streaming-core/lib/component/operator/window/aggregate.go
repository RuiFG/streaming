package window

import (
	. "streaming/streaming-core/lib/component/operator"
	"streaming/streaming-core/lib/element"
	store2 "streaming/streaming-core/lib/store"
)

type Operator[IN, ACC, OUT any, KEY comparable] struct {
	Default[IN, any, OUT]
	KeySelectorFn[IN, KEY]
	AggregateFn[IN, ACC, OUT]
	AssignerFn[IN]
	TriggerFn[IN]
	Fn[ACC, OUT, KEY]
	Serializer   store2.MapSerializer[KEY, ACC]
	Deserializer store2.MapDeserializer[KEY, ACC]
}

func (a *Operator[IN, ACC, OUT, K]) ProcessEvent1(event *element.Event[IN]) {
	key := a.KeySelectorFn(event.Value)
	windows := a.assignWindows(event.Value)
	for _, window := range windows {
		mapState, err := store2.MMapState(a.Ctx.Store(), store2.MapStateDescriptor[K, ACC]{
			Key:          window.Key(),
			Serializer:   a.Serializer,
			Deserializer: a.Deserializer,
		})
		if err != nil {
			continue
		} else {
			if load, ok := mapState.Load(key); !ok {
				mapState.Store(key, a.Init(event.Value))
			} else {
				mapState.Store(key, a.Add(event.Value, load))
			}
		}
		triggerResult := a.TriggerFn.onElement(event.Value)

		if triggerResult.IsFire() {
			if acc, ok := mapState.Load(key); !ok {
				continue
			} else {
				outputs := a.Fn(key, window, []ACC{acc})
				for _, out := range outputs {
					a.Collector(out)
				}
			}
		}
		if triggerResult.IsPurge() {
			mapState.Clear()
		}
	}

}
