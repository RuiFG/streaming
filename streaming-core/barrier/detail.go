package barrier

type Type uint

const (
	CheckpointBarrier Type = iota
	ExitpointBarrier
)

type Detail struct {
	Id int64
	Type
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
