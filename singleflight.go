package pantry

import (
	"fmt"
	"sync"
)

type call[V any] struct {
	done  chan struct{}
	value V
	err   error
}

type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
}

func newFlightGroup[K comparable, V any]() *flightGroup[K, V] {
	return &flightGroup[K, V]{calls: make(map[K]*call[V])}
}

func (g *flightGroup[K, V]) do(key K, fn func() (V, error)) (V, bool, error) {
	g.mu.Lock()
	if existing, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-existing.done
		return existing.value, true, existing.err
	}
	c := &call[V]{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = fmt.Errorf("pantry: loader panicked: %v", r)
			}
		}()
		c.value, c.err = fn()
	}()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	close(c.done)
	return c.value, false, c.err
}

func (g *flightGroup[K, V]) inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}
