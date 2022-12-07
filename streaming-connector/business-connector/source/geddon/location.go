package geddon

import (
	"container/heap"
	"path"
)

type Location struct {
	AbsolutePath string
	Offset       int64
}

func (f Location) Filename() string {
	return path.Base(f.AbsolutePath)
}

func (f Location) Ext() string {
	return path.Ext(f.AbsolutePath)
}

var emptyLocation = Location{Offset: 0, AbsolutePath: ""}

type Queue interface {
	Len() int
	PushLocation(location Location)
	PopLocation() Location
}

type PriorityQueue struct {
	items      []Location
	comparator ComparatorFn
}

func (q *PriorityQueue) Len() int {
	return len(q.items)
}

func (q *PriorityQueue) Less(i, j int) bool {
	return q.comparator(q.items[i], q.items[j])
}

func (q *PriorityQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *PriorityQueue) Push(x any) {
	item := x.(Location)
	q.items = append(q.items, item)
}

func (q *PriorityQueue) Pop() any {
	old := q.items
	n := len(old)
	x := old[n-1]
	q.items = old[0 : n-1]
	return x
}

func (q *PriorityQueue) PushLocation(location Location) {
	heap.Push(q, location)
}

func (q *PriorityQueue) PopLocation() Location {
	return q.Pop().(Location)
}

type SliceQueue []Location

func (s *SliceQueue) Len() int {
	return len(*s)
}

func (s *SliceQueue) PushLocation(location Location) {
	*s = append(*s, location)
}

func (s *SliceQueue) PopLocation() Location {
	location := (*s)[0]
	*s = (*s)[1:]
	return location
}
