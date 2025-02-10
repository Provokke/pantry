package pantry

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key     K
	value   V
	expires time.Time
}

func (e *entry[K, V]) expired(now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

type event[K comparable, V any] struct {
	key    K
	value  V
	reason Reason
}

type shard[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	items    map[K]*list.Element
	order    *list.List
}

func newShard[K comparable, V any](capacity int) *shard[K, V] {
	return &shard[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		order:    list.New(),
	}
}

func (s *shard[K, V]) get(key K, now time.Time) (V, bool, []event[K, V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, ok := s.items[key]
	if !ok {
		var zero V
		return zero, false, nil
	}
	e := element.Value.(*entry[K, V])
	if e.expired(now) {
		s.remove(element)
		var zero V
		return zero, false, []event[K, V]{{e.key, e.value, Expired}}
	}
	s.order.MoveToFront(element)
	return e.value, true, nil
}

func (s *shard[K, V]) peek(key K, now time.Time) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, ok := s.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	e := element.Value.(*entry[K, V])
	if e.expired(now) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (s *shard[K, V]) set(key K, value V, expires time.Time, now time.Time) []event[K, V] {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []event[K, V]
	if element, ok := s.items[key]; ok {
		e := element.Value.(*entry[K, V])
		events = append(events, event[K, V]{e.key, e.value, Replaced})
		e.value = value
		e.expires = expires
		s.order.MoveToFront(element)
		return events
	}
	s.items[key] = s.order.PushFront(&entry[K, V]{key: key, value: value, expires: expires})
	for s.order.Len() > s.capacity {
		events = append(events, s.evictOne(now))
	}
	return events
}

func (s *shard[K, V]) evictOne(now time.Time) event[K, V] {
	victim := s.order.Back()
	for element := s.order.Back(); element != nil; element = element.Prev() {
		if element.Value.(*entry[K, V]).expired(now) {
			victim = element
			break
		}
	}
	e := victim.Value.(*entry[K, V])
	s.remove(victim)
	if e.expired(now) {
		return event[K, V]{e.key, e.value, Expired}
	}
	return event[K, V]{e.key, e.value, Evicted}
}

func (s *shard[K, V]) delete(key K) (event[K, V], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, ok := s.items[key]
	if !ok {
		return event[K, V]{}, false
	}
	e := element.Value.(*entry[K, V])
	s.remove(element)
	return event[K, V]{e.key, e.value, Deleted}, true
}

func (s *shard[K, V]) remove(element *list.Element) {
	e := s.order.Remove(element).(*entry[K, V])
	delete(s.items, e.key)
}

func (s *shard[K, V]) sweep(now time.Time) []event[K, V] {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []event[K, V]
	for element := s.order.Back(); element != nil; {
		previous := element.Prev()
		e := element.Value.(*entry[K, V])
		if e.expired(now) {
			s.remove(element)
			events = append(events, event[K, V]{e.key, e.value, Expired})
		}
		element = previous
	}
	return events
}

func (s *shard[K, V]) purge() []event[K, V] {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]event[K, V], 0, s.order.Len())
	for element := s.order.Front(); element != nil; element = element.Next() {
		e := element.Value.(*entry[K, V])
		events = append(events, event[K, V]{e.key, e.value, Deleted})
	}
	s.items = make(map[K]*list.Element)
	s.order.Init()
	return events
}

func (s *shard[K, V]) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

func (s *shard[K, V]) keys(now time.Time) []K {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]K, 0, s.order.Len())
	for element := s.order.Front(); element != nil; element = element.Next() {
		e := element.Value.(*entry[K, V])
		if !e.expired(now) {
			keys = append(keys, e.key)
		}
	}
	return keys
}

