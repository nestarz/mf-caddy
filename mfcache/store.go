package mfcache

import (
	"container/list"
	"maps"
	"net/http"
	"sync"
	"time"
)

// entryOverhead is what an entry costs beyond its bytes: the list element, maps and slices.
const entryOverhead = 512

// entryShare is the part of the bound one entry may take: 1/16, 16 MiB of 256 MiB.
const entryShare = 16

// entry is one stored response. It is never changed once stored.
type entry struct {
	key        string
	status     int
	header     http.Header
	body       []byte
	vary       map[string]string
	lifetime   time.Duration
	initialAge time.Duration
	responded  time.Time
	size       int64
}

// age is the current_age of RFC 9111 §4.2.3.
func (e *entry) age(now time.Time) time.Duration {
	return e.initialAge + max(0, now.Sub(e.responded))
}

// fresh reports that the entry may still be served.
func (e *entry) fresh(now time.Time) bool {
	return e.lifetime > e.age(now)
}

// matches reports that the request selects this entry's variant (RFC 9111 §4.1).
func (e *entry) matches(request http.Header) bool {
	for name, value := range e.vary {
		if fieldValue(request, name) != value {
			return false
		}
	}
	return true
}

// bytes is what the entry holds in memory, as the bound counts it.
func (e *entry) bytes() int64 {
	n := int64(len(e.key) + len(e.body) + entryOverhead)
	for name, values := range e.header {
		for _, v := range values {
			n += int64(len(name) + len(v))
		}
	}
	for name, value := range e.vary {
		n += int64(len(name) + len(value))
	}
	return n
}

// store holds entries by primary key, evicting the least recently used beyond max bytes.
type store struct {
	mu    sync.Mutex
	max   int64
	size  int64
	lru   *list.List
	byKey map[string][]*list.Element
}

// newStore makes an empty store bounded at bound bytes.
func newStore(bound int64) *store {
	return &store{max: bound, lru: list.New(), byKey: map[string][]*list.Element{}}
}

// maxEntry is the largest entry the store accepts.
func (s *store) maxEntry() int {
	return int(s.max / entryShare)
}

// lookup returns a fresh entry for key whose variant the request selects, dropping stale ones.
// Without one, it names the RFC 9211 fwd reason: uri-miss, vary-miss or stale.
func (s *store) lookup(key string, request http.Header, now time.Time) (*entry, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elements := s.byKey[key]
	if len(elements) == 0 {
		return nil, "uri-miss"
	}
	fresh := false
	for _, el := range elements {
		e := el.Value.(*entry)
		if !e.fresh(now) {
			s.remove(el)
			continue
		}
		fresh = true
		if e.matches(request) {
			s.lru.MoveToFront(el)
			return e, ""
		}
	}
	if fresh {
		return nil, "vary-miss"
	}
	return nil, "stale"
}

// put stores e in place of the entry for the same variant, then evicts down to the bound.
func (s *store) put(e *entry) {
	if e.size > int64(s.maxEntry()) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, el := range s.byKey[e.key] {
		if maps.Equal(el.Value.(*entry).vary, e.vary) {
			s.remove(el)
			break
		}
	}
	s.byKey[e.key] = append(s.byKey[e.key], s.lru.PushFront(e))
	s.size += e.size
	for s.size > s.max {
		s.remove(s.lru.Back())
	}
}

// invalidate drops every entry for key.
func (s *store) invalidate(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, el := range s.byKey[key] {
		s.remove(el)
	}
}

// remove drops one element; the caller holds the lock.
func (s *store) remove(el *list.Element) {
	e := s.lru.Remove(el).(*entry)
	s.size -= e.size
	rest := s.byKey[e.key][:0:0]
	for _, other := range s.byKey[e.key] {
		if other != el {
			rest = append(rest, other)
		}
	}
	if len(rest) == 0 {
		delete(s.byKey, e.key)
	} else {
		s.byKey[e.key] = rest
	}
}

// Destruct empties the store when no handler uses it any more.
func (s *store) Destruct() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Init()
	s.byKey = map[string][]*list.Element{}
	s.size = 0
	return nil
}
