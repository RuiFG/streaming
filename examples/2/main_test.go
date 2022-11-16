package main

import "testing"

type G[T any] struct {
	v T
}
type GG[T any] struct {
	g G[T]
}

func Emit1[T any](t any) G[T] {
	switch tV := t.(type) {
	case G[T]:
		return tV
	}
	return t.(G[T])
}
func Emit2[T any](gg GG[T]) G[T] {
	return gg.g
}

func BenchmarkGenerics(b *testing.B) {
	g := G[string]{v: "asd"}
	for i := 0; i < b.N; i++ {
		Emit1[string](g)
	}
}

func BenchmarkGenerics2(b *testing.B) {
	g := GG[string]{G[string]{"asd"}}
	for i := 0; i < b.N; i++ {
		Emit2[string](g)
	}
}
