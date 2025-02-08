<div align="center">

# pantry

A generic in-memory cache for Go with TTLs, LRU eviction, sharding and singleflight loading. Standard library only.

![License](https://img.shields.io/badge/LICENSE-MIT-blue?style=for-the-badge)  ![Language](https://img.shields.io/badge/GO-00ADD8?style=for-the-badge&logo=go&logoColor=white)  ![Tests](https://img.shields.io/badge/TESTS-24-brightgreen?style=for-the-badge)

Refer to [LICENSE](LICENSE) for the legal bits.

</div>

## What's in it

- `Cache[K comparable, V any]`, so keys and values keep their real types and there is no `interface{}` boxing.
- LRU eviction per shard. When a shard is full it drops an expired entry first if it has one, and the least recently used entry otherwise.
- A default TTL and a per-entry override with `SetWithTTL`. Expired entries are removed lazily on `Get`, or in bulk with `Sweep`.
- Sharding by `hash/maphash`, so goroutines touching different keys rarely contend on the same mutex.
- `GetOrLoad`, which collapses concurrent loads of the same missing key into one call to your loader. A loader error is returned to every waiter and never cached, and a panicking loader becomes an error instead of taking the process down.
- An `OnRemove` callback that says why an entry went: evicted, expired, deleted or replaced. It runs outside the shard lock, so it is safe to call back into the cache.
- Counters for hits, misses, evictions, expirations and loads.
- An injectable clock, which is what makes the TTL tests deterministic.

```go
c, _ := pantry.New(pantry.Options[string, User]{
	Capacity: 10_000,
	Shards:   16,
	TTL:      5 * time.Minute,
})

user, err := c.GetOrLoad("u-42", func(id string) (User, error) {
	return db.FindUser(id)
})
```

## 🏗️ Architecture

```
 Cache[K,V]
  ├─ shards[]  chosen by maphash(key) % n
  │     └─ shard: mutex + map[K]*list.Element + container/list (front = most recent)
  │           └─ entry{key, value, expires}
  ├─ flightGroup  map[K]*call  (one in-flight load per key)
  └─ atomic counters  hits, misses, evictions, expirations, loads
```

- **pantry.go** is the public API: `New`, `Get`, `Peek`, `Set`, `SetWithTTL`, `Delete`, `Sweep`, `Purge`, `Keys`, `GetOrLoad` and `Stats`. Capacity is split evenly across shards, rounded up.
- **shard.go** is one locked LRU. It returns removal events to the caller rather than calling back while holding its lock.
- **singleflight.go** is a small generic singleflight group. The first caller for a key runs the function, later callers wait on a channel and share the result.

`Peek` reads without touching recency or the hit and miss counters.

## 🚀 Quick start

Go 1.24 or newer (`maphash.Comparable` needs it).

```
go get github.com/Provokke/pantry
go test -race ./...
```

```go
c, _ := pantry.New(pantry.Options[string, int]{Capacity: 2, Shards: 1})
c.Set("a", 1)
c.Set("b", 2)
c.Get("a")
c.Set("c", 3)

_, ok := c.Get("b")
fmt.Println(ok, c.Stats().Evictions)
```

```
false 1
```

## 🧪 Testing

```
go test -race -count=1 ./...
```

24 tests, all passing under the race detector:

- Options: bad capacity and TTL, shard count clamped to capacity (2).
- Basics: set, get, overwrite, delete and purge, keys, hit rate, reason names (6).
- LRU: eviction order, `Peek` not refreshing recency, expired entries evicted first (3).
- TTL: default and per-entry expiry, `Sweep`, expiry counters (3).
- Callbacks: replaced, evicted, deleted and expired reasons (1).
- Loading: result cached, errors not cached, 40 goroutines sharing one load, panic recovery (4).
- Singleflight on its own: shared runs, separate keys, errors, panics (4).
- A mixed concurrent workload over eight goroutines (1).
