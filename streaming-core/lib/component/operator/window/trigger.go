package window

import (
	"fmt"
	"time"
)

type TriggerResult int

func (t TriggerResult) IsPurge() bool {
	return t&2 == 1
}

func (t TriggerResult) IsFire() bool {
	return t&1 == 1
}

const (
	Continue     TriggerResult = 0
	Fire         TriggerResult = 1
	Purge        TriggerResult = 2
	FireAndPurge TriggerResult = 3
)

type Window struct {
	start time.Time
	end   time.Time
}

func (w Window) Key() string {
	return fmt.Sprintf("%d%d", w.start.Unix(), w.end.Unix())
}
