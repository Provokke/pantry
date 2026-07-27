package pantry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func mustNew[K comparable, V any](t *testing.T, opts Options[K, V]) *Cache[K, V] {
	t.Helper()
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewRejectsBadOptions(t *testing.T) {
	if _, err := New(Options[string, int]{Capacity: 0}); err == nil {
		t.Fatal("expected an error for zero capacity")
	}
	if _, err := New(Options[string, int]{Capacity: 4, TTL: -time.Second}); err == nil {
		t.Fatal("expected an error for negative ttl")
	}
}

func TestShardsAreClampedToCapacity(t *testing.T) {
	c := mustNew(t, Options[int, int]{Capacity: 3, Shards: 50})
	if len(c.shards) != 3 {
		t.Fatalf("shards = %d, want 3", len(c.shards))
	}
}

func TestSetAndGet(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1})
	c.Set("a", 1)
	c.Set("b", 2)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("unexpected hit")
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d", c.Len())
	}
}

func TestOverwriteKeepsOneEntry(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1})
	c.Set("a", 1)
	c.Set("a", 2)
	if v, _ := c.Get("a"); v != 2 {
		t.Fatalf("value = %d, want 2", v)
	}
	if c.Len() != 1 {
		t.Fatalf("Len = %d, want 1", c.Len())
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 3, Shards: 1})
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Get("a")
	c.Set("d", 4)
	if _, ok := c.Peek("b"); ok {
		t.Fatal("b should have been evicted")
	}
	for _, key := range []string{"a", "c", "d"} {
		if _, ok := c.Peek(key); !ok {
			t.Fatalf("%s should still be present", key)
		}
	}
	if got := c.Stats().Evictions; got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
}

func TestPeekDoesNotRefreshRecency(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 2, Shards: 1})
	c.Set("a", 1)
	c.Set("b", 2)
	c.Peek("a")
	c.Set("c", 3)
	if _, ok := c.Peek("a"); ok {
		t.Fatal("a was refreshed by Peek")
	}
}

func TestTTLExpiry(t *testing.T) {
	clock := newClock()
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1, TTL: time.Minute, Clock: clock.Now})
	c.Set("a", 1)
	clock.Advance(59 * time.Second)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expired too early")
	}
	clock.Advance(time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatal("should have expired")
	}
	if got := c.Stats().Expirations; got != 1 {
		t.Fatalf("expirations = %d", got)
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d after expiry", c.Len())
	}
}

func TestPerEntryTTLOverridesDefault(t *testing.T) {
	clock := newClock()
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1, TTL: time.Hour, Clock: clock.Now})
	c.SetWithTTL("short", 1, time.Second)
	c.SetWithTTL("forever", 2, 0)
	c.Set("default", 3)
	clock.Advance(time.Minute)
	if _, ok := c.Get("short"); ok {
		t.Fatal("short should have expired")
	}
	if _, ok := c.Get("forever"); !ok {
		t.Fatal("forever should not expire")
	}
	if _, ok := c.Get("default"); !ok {
		t.Fatal("default should still be live")
	}
}

func TestEvictionPrefersExpiredEntries(t *testing.T) {
	clock := newClock()
	c := mustNew(t, Options[string, int]{Capacity: 3, Shards: 1, Clock: clock.Now})
	c.SetWithTTL("old", 1, time.Second)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Get("old")
	clock.Advance(2 * time.Second)
	c.Set("d", 4)
	for _, key := range []string{"b", "c", "d"} {
		if _, ok := c.Peek(key); !ok {
			t.Fatalf("%s should have survived", key)
		}
	}
	if got := c.Stats().Expirations; got != 1 {
		t.Fatalf("expirations = %d, want 1", got)
	}
}

func TestSweepRemovesExpired(t *testing.T) {
	clock := newClock()
	c := mustNew(t, Options[int, int]{Capacity: 100, Shards: 4, TTL: time.Second, Clock: clock.Now})
	for i := 0; i < 20; i++ {
		c.Set(i, i)
	}
	c.SetWithTTL(99, 99, time.Hour)
	clock.Advance(2 * time.Second)
	if removed := c.Sweep(); removed != 20 {
		t.Fatalf("removed = %d, want 20", removed)
	}
	if c.Len() != 1 {
		t.Fatalf("Len = %d, want 1", c.Len())
	}
}

func TestDeleteAndPurge(t *testing.T) {
	c := mustNew(t, Options[int, int]{Capacity: 10, Shards: 2})
	for i := 0; i < 5; i++ {
		c.Set(i, i)
	}
	if !c.Delete(2) || c.Delete(2) {
		t.Fatal("Delete should report true once")
	}
	c.Purge()
	if c.Len() != 0 {
		t.Fatalf("Len = %d after purge", c.Len())
	}
}

func TestKeysSkipsExpired(t *testing.T) {
	clock := newClock()
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 2, Clock: clock.Now})
	c.SetWithTTL("gone", 1, time.Second)
	c.Set("kept", 2)
	clock.Advance(time.Minute)
	keys := c.Keys()
	sort.Strings(keys)
	if len(keys) != 1 || keys[0] != "kept" {
		t.Fatalf("keys = %v", keys)
	}
}

func TestOnRemoveReasons(t *testing.T) {
	clock := newClock()
	var mu sync.Mutex
	seen := map[string]Reason{}
	c := mustNew(t, Options[string, int]{
		Capacity: 2, Shards: 1, TTL: time.Minute, Clock: clock.Now,
		OnRemove: func(key string, value int, reason Reason) {
			mu.Lock()
			seen[fmt.Sprintf("%s/%s", key, reason)] = reason
			mu.Unlock()
		},
	})
	c.Set("a", 1)
	c.Set("a", 2)
	c.Set("b", 3)
	c.Set("c", 4)
	c.Delete("c")
	c.SetWithTTL("e", 5, time.Second)
	clock.Advance(time.Hour)
	c.Sweep()
	for _, want := range []string{"a/replaced", "a/evicted", "c/deleted", "e/expired"} {
		if _, ok := seen[want]; !ok {
			t.Errorf("missing callback %s; got %v", want, seen)
		}
	}
}

func TestGetOrLoadCachesResult(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1})
	calls := 0
	loader := func(key string) (int, error) {
		calls++
		return len(key), nil
	}
	for i := 0; i < 3; i++ {
		v, err := c.GetOrLoad("hello", loader)
		if err != nil || v != 5 {
			t.Fatalf("GetOrLoad = %v, %v", v, err)
		}
	}
	if calls != 1 {
		t.Fatalf("loader ran %d times, want 1", calls)
	}
}

func TestGetOrLoadErrorIsNotCached(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 1})
	boom := errors.New("boom")
	if _, err := c.GetOrLoad("k", func(string) (int, error) { return 0, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	v, err := c.GetOrLoad("k", func(string) (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("retry = %v, %v", v, err)
	}
	if got := c.Stats().LoadErrors; got != 1 {
		t.Fatalf("load errors = %d", got)
	}
}

func TestGetOrLoadCollapsesConcurrentLoads(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 10, Shards: 4})
	var calls atomic.Int32
	release := make(chan struct{})
	loader := func(string) (int, error) {
		calls.Add(1)
		<-release
		return 42, nil
	}
	const workers = 40
	var wg sync.WaitGroup
	results := make(chan int, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.GetOrLoad("slow", loader)
			if err != nil {
				t.Error(err)
			}
			results <- v
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.flight.inflight() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)
	for v := range results {
		if v != 42 {
			t.Fatalf("got %d", v)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times, want 1", got)
	}
	if c.Stats().SharedLoads == 0 {
		t.Fatal("expected some shared loads")
	}
}

func TestLoaderPanicBecomesError(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 4, Shards: 1})
	_, err := c.GetOrLoad("p", func(string) (int, error) { panic("oh no") })
	if err == nil {
		t.Fatal("expected an error")
	}
	if c.flight.inflight() != 0 {
		t.Fatal("flight entry leaked after panic")
	}
}

func TestHitRate(t *testing.T) {
	c := mustNew(t, Options[string, int]{Capacity: 4, Shards: 1})
	if c.Stats().HitRate() != 0 {
		t.Fatal("empty cache hit rate should be 0")
	}
	c.Set("a", 1)
	c.Get("a")
	c.Get("b")
	if got := c.Stats().HitRate(); got != 0.5 {
		t.Fatalf("hit rate = %v", got)
	}
}

func TestConcurrentMixedUse(t *testing.T) {
	c := mustNew(t, Options[int, int]{Capacity: 64, Shards: 8, TTL: time.Minute})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := (i * (w + 1)) % 200
				switch i % 4 {
				case 0:
					c.Set(key, i)
				case 1:
					c.Get(key)
				case 2:
					c.GetOrLoad(key, func(k int) (int, error) { return k, nil })
				default:
					c.Delete(key)
				}
			}
		}(w)
	}
	wg.Wait()
	if c.Len() > 64+8 {
		t.Fatalf("Len = %d exceeds capacity", c.Len())
	}
}

func TestReasonString(t *testing.T) {
	names := map[Reason]string{Evicted: "evicted", Expired: "expired", Deleted: "deleted", Replaced: "replaced", Reason(9): "unknown"}
	for reason, want := range names {
		if reason.String() != want {
			t.Errorf("%d.String() = %q", reason, reason.String())
		}
	}
}
