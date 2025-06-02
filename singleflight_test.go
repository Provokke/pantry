package pantry

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlightRunsOnceForSharedKey(t *testing.T) {
	g := newFlightGroup[string, int]()
	var calls atomic.Int32
	var shared atomic.Int32
	gate := make(chan struct{})
	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		v, wasShared, err := g.do("k", func() (int, error) {
			calls.Add(1)
			<-gate
			return 9, nil
		})
		if err != nil || v != 9 {
			t.Errorf("do = %d, %v", v, err)
		}
		if wasShared {
			shared.Add(1)
		}
	}
	wg.Add(1)
	go run()
	for g.inflight() == 0 {
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go run()
	}
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("function ran %d times, want 1", got)
	}
	if got := shared.Load(); got != 9 {
		t.Fatalf("shared = %d, want 9", got)
	}
}

func TestFlightDifferentKeysRunSeparately(t *testing.T) {
	g := newFlightGroup[int, int]()
	for i := 0; i < 3; i++ {
		v, shared, err := g.do(i, func() (int, error) { return i * 2, nil })
		if v != i*2 || shared || err != nil {
			t.Fatalf("do(%d) = %d, %v, %v", i, v, shared, err)
		}
	}
}

func TestFlightPropagatesError(t *testing.T) {
	g := newFlightGroup[string, int]()
	want := errors.New("nope")
	_, _, err := g.do("k", func() (int, error) { return 0, want })
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
	if g.inflight() != 0 {
		t.Fatal("call entry not cleaned up")
	}
}

func TestFlightRecoversPanic(t *testing.T) {
	g := newFlightGroup[string, int]()
	_, _, err := g.do("k", func() (int, error) { panic("bad") })
	if err == nil {
		t.Fatal("expected error from panic")
	}
	v, _, err := g.do("k", func() (int, error) { return 1, nil })
	if err != nil || v != 1 {
		t.Fatalf("group unusable after panic: %d, %v", v, err)
	}
}

