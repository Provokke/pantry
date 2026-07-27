package pantry

import (
	"errors"
	"hash/maphash"
	"sync/atomic"
	"time"
)

type Reason int

const (
	Evicted Reason = iota
	Expired
	Deleted
	Replaced
)

func (r Reason) String() string {
	switch r {
	case Evicted:
		return "evicted"
	case Expired:
		return "expired"
	case Deleted:
		return "deleted"
	case Replaced:
		return "replaced"
	}
	return "unknown"
}

type Options[K comparable, V any] struct {
	Capacity int
	Shards   int
	TTL      time.Duration
	Clock    func() time.Time
	OnRemove func(key K, value V, reason Reason)
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Expirations uint64
	Loads       uint64
	SharedLoads uint64
	LoadErrors  uint64
}

func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

type Cache[K comparable, V any] struct {
	shards   []*shard[K, V]
	seed     maphash.Seed
	ttl      time.Duration
	clock    func() time.Time
	onRemove func(K, V, Reason)
	flight   *flightGroup[K, V]

	hits, misses, evictions, expirations atomic.Uint64
	loads, sharedLoads, loadErrors       atomic.Uint64
}

var ErrBadOptions = errors.New("pantry: capacity must be positive and shards must not exceed capacity")

func New[K comparable, V any](opts Options[K, V]) (*Cache[K, V], error) {
	if opts.Capacity <= 0 {
		return nil, ErrBadOptions
	}
	shards := opts.Shards
	if shards <= 0 {
		shards = 16
	}
	if shards > opts.Capacity {
		shards = opts.Capacity
	}
	if opts.TTL < 0 {
		return nil, errors.New("pantry: ttl must not be negative")
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	perShard := (opts.Capacity + shards - 1) / shards
	c := &Cache[K, V]{
		shards:   make([]*shard[K, V], shards),
		seed:     maphash.MakeSeed(),
		ttl:      opts.TTL,
		clock:    clock,
		onRemove: opts.OnRemove,
		flight:   newFlightGroup[K, V](),
	}
	for i := range c.shards {
		c.shards[i] = newShard[K, V](perShard)
	}
	return c, nil
}

func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	index := maphash.Comparable(c.seed, key) % uint64(len(c.shards))
	return c.shards[index]
}

func (c *Cache[K, V]) dispatch(events []event[K, V]) {
	for _, e := range events {
		switch e.reason {
		case Evicted:
			c.evictions.Add(1)
		case Expired:
			c.expirations.Add(1)
		}
		if c.onRemove != nil {
			c.onRemove(e.key, e.value, e.reason)
		}
	}
}

func (c *Cache[K, V]) expiry(ttl time.Duration, now time.Time) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	value, ok, events := c.shardFor(key).get(key, c.clock())
	c.dispatch(events)
	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return value, ok
}

func (c *Cache[K, V]) Peek(key K) (V, bool) {
	return c.shardFor(key).peek(key, c.clock())
}

func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, c.ttl)
}

func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	now := c.clock()
	c.dispatch(c.shardFor(key).set(key, value, c.expiry(ttl, now), now))
}

func (c *Cache[K, V]) Delete(key K) bool {
	e, ok := c.shardFor(key).delete(key)
	if ok {
		c.dispatch([]event[K, V]{e})
	}
	return ok
}

func (c *Cache[K, V]) Len() int {
	total := 0
	for _, s := range c.shards {
		total += s.len()
	}
	return total
}

func (c *Cache[K, V]) Keys() []K {
	now := c.clock()
	var keys []K
	for _, s := range c.shards {
		keys = append(keys, s.keys(now)...)
	}
	return keys
}

func (c *Cache[K, V]) Sweep() int {
	now := c.clock()
	removed := 0
	for _, s := range c.shards {
		events := s.sweep(now)
		removed += len(events)
		c.dispatch(events)
	}
	return removed
}

func (c *Cache[K, V]) Purge() {
	for _, s := range c.shards {
		c.dispatch(s.purge())
	}
}

func (c *Cache[K, V]) GetOrLoad(key K, loader func(K) (V, error)) (V, error) {
	if value, ok := c.Get(key); ok {
		return value, nil
	}
	value, shared, err := c.flight.do(key, func() (V, error) {
		if existing, ok := c.Peek(key); ok {
			return existing, nil
		}
		c.loads.Add(1)
		loaded, err := loader(key)
		if err != nil {
			c.loadErrors.Add(1)
			var zero V
			return zero, err
		}
		c.Set(key, loaded)
		return loaded, nil
	})
	if shared {
		c.sharedLoads.Add(1)
	}
	return value, err
}

func (c *Cache[K, V]) Stats() Stats {
	return Stats{
		Hits:        c.hits.Load(),
		Misses:      c.misses.Load(),
		Evictions:   c.evictions.Load(),
		Expirations: c.expirations.Load(),
		Loads:       c.loads.Load(),
		SharedLoads: c.sharedLoads.Load(),
		LoadErrors:  c.loadErrors.Load(),
	}
}
