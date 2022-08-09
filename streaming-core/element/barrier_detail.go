package element

type BarrierType uint

const (
	CheckpointBarrier BarrierType = iota
	ExitpointBarrier
)

type Detail struct {
	Id int64
	BarrierType
}

type Message uint

const (
	ACK Message = iota
	DEC
)

type Signal struct {
	Name string
	Message
	Detail
}
