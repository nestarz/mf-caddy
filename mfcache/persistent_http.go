package mfcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/prometheus/client_golang/prometheus"
)

type cacheMetrics struct {
	ramHits, s3Hits, cdnHits, streamed, fills, skipped, errors, evictions, deleted, purges atomic.Uint64
}

func newCacheMetrics() *cacheMetrics { return &cacheMetrics{} }

func (m *cacheMetrics) counters() map[string]*atomic.Uint64 {
	return map[string]*atomic.Uint64{"ram_hits": &m.ramHits, "s3_hits": &m.s3Hits, "cdn_hits": &m.cdnHits,
		"streamed_bytes": &m.streamed, "fills": &m.fills, "skipped_fills": &m.skipped, "errors": &m.errors,
		"evictions": &m.evictions, "deleted_objects": &m.deleted, "purges": &m.purges}
}
func (m *cacheMetrics) Describe(ch chan<- *prometheus.Desc) {
	for name := range m.counters() {
		ch <- prometheus.NewDesc("mf_cache_"+name+"_total", "Persistent HTTP cache "+name, nil, nil)
	}
}
func (m *cacheMetrics) Collect(ch chan<- prometheus.Metric) {
	for name, value := range m.counters() {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("mf_cache_"+name+"_total", "Persistent HTTP cache "+name, nil, nil), prometheus.CounterValue, float64(value.Load()))
	}
}

func reusable(r *http.Request, e *entry) bool {
	if r.Header.Get("Authorization") == "" {
		return true
	}
	cc := directives(e.header.Values("Cache-Control"))
	return has(cc, "public") || has(cc, "s-maxage")
}

func (h *Handler) servePersistent(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	return h.servePersistentAttempt(w, r, next, true, nil)
}

func (h *Handler) servePersistentAttempt(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler, coordinate bool, ready func()) error {
	p := h.persistent
	key := h.Scope + "\x00" + primaryKey(r)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		c := newCapture(w, "mf; fwd=method", func(int, http.Header) (string, bool) { return "", false })
		if err := next.ServeHTTP(c, r); err != nil {
			return err
		}
		c.finish()
		if unsafeMethod(r.Method) && c.status >= 200 && c.status < 400 {
			if err := p.invalidate(func(e cachedResponse) bool { return e.Key == key }); err != nil {
				return err
			}
		}
		return nil
	}
	// Request no-store forbids both reuse and population, in either cache tier.
	if has(directives(r.Header.Values("Cache-Control")), "no-store") {
		return next.ServeHTTP(w, r)
	}
	now := h.now()
	p.mu.RLock()
	ram, _ := p.ram.lookup(key, r.Header, now)
	if p.invalid {
		ram = nil
	}
	p.mu.RUnlock()
	if ram != nil && reusable(r, ram) {
		p.metrics.ramHits.Add(1)
		serve(w, r, ram, now)
		return nil
	}
	e, epoch, err := p.lookup(key, r.Header, now)
	if err != nil {
		p.metrics.errors.Add(1)
	}
	if e != nil && reusable(r, e.entry()) {
		select {
		case p.reads <- struct{}{}:
			defer func() { <-p.reads }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Cache delivery is busy. Please retry.", 503)
			return nil
		}
		if r.Method == http.MethodHead {
			writeCachedHeader(w, e, now, "s3")
			return nil
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		body, source, err := p.openBody(ctx, *e)
		if err == nil {
			defer body.Close()
			writeCachedHeader(w, e, now, source)
			// Only explicitly small objects can enter RAM. Large reads retain one bounded buffer.
			var small bytes.Buffer
			var dst io.Writer = w
			if e.Bytes <= p.config.RAMEntryBytes && e.Bytes <= int64(p.ram.maxEntry()) && p.config.RAMBytes > 0 {
				dst = io.MultiWriter(w, &small)
			}
			buffer := make([]byte, p.config.StreamBufferBytes)
			n, err := io.CopyBuffer(dst, io.LimitReader(body, e.Bytes), buffer)
			p.metrics.streamed.Add(uint64(n))
			if source == "cdn" {
				p.metrics.cdnHits.Add(1)
			} else {
				p.metrics.s3Hits.Add(1)
			}
			if err != nil {
				return err
			}
			if n != e.Bytes {
				return io.ErrUnexpectedEOF
			}
			if dst != w {
				p.mu.Lock()
				if p.epoch == epoch {
					cached := e.entry()
					cached.body = small.Bytes()
					cached.vary = make(map[string]string, len(e.Vary))
					for name := range e.Vary {
						cached.vary[name] = fieldValue(r.Header, name)
					}
					cached.size = cached.bytes()
					p.ram.put(cached)
				}
				p.mu.Unlock()
			}
			return nil
		}
		p.metrics.errors.Add(1)
	}
	if coordinate && r.Method == http.MethodGet && (r.Body == nil || r.Body == http.NoBody) {
		return h.coalesceFill(w, r, next, key, epoch)
	}
	return h.fillPersistent(w, r, next, key, epoch, now, ready)
}

func writeCachedHeader(w http.ResponseWriter, e *cachedResponse, now time.Time, source string) {
	for name, values := range e.Header {
		w.Header()[name] = append([]string(nil), values...)
	}
	w.Header().Set("Age", strconv.FormatInt(int64(e.entry().age(now)/time.Second), 10))
	w.Header().Add("Cache-Status", "mf; hit; detail="+source)
	w.Header().Set("Content-Length", strconv.FormatInt(e.Bytes, 10))
	w.WriteHeader(e.Status)
}

func (p *persistentStore) openBody(ctx context.Context, e cachedResponse) (io.ReadCloser, string, error) {
	if e.Public && p.cdnURL != "" {
		body, err := openCDN(ctx, p.cdnClient, p.cdnURL, e.Object, e.Bytes)
		if err == nil {
			return body, "cdn", nil
		}
		p.metrics.errors.Add(1)
	}
	backend := p.backend(e.Public)
	if backend == nil {
		return nil, "", errors.New("cache body store unavailable")
	}
	body, size, err := backend.Open(ctx, e.Object)
	if err != nil {
		return nil, "", err
	}
	if size != e.Bytes {
		body.Close()
		return nil, "", errors.New("cache object size mismatch")
	}
	return body, "s3", nil
}

// publicBody is an explicit opt-in, never a filename or extension heuristic. Even cacheable
// cookie/auth variants stay in the private store and cannot be fetched at the public body URL.
func publicBody(r *http.Request, d decision) bool {
	if !has(directives(d.header.Values("Cache-Control")), "public") || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
		return false
	}
	for _, name := range d.vary {
		if name != "Accept-Encoding" {
			return false
		}
	}
	return d.detail == ""
}

func (h *Handler) fillPersistent(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler, key string, epoch uint64, requested time.Time, ready func()) error {
	p := h.persistent
	if r.Method != http.MethodGet {
		return next.ServeHTTP(w, r)
	}
	select {
	case p.fills <- struct{}{}:
	default:
		p.metrics.skipped.Add(1)
		return next.ServeHTTP(w, r)
	}
	defer func() { <-p.fills }()
	file, err := os.CreateTemp(filepath.Dir(p.config.IndexPath), filepath.Base(p.config.IndexPath)+".fill-*")
	if err != nil {
		p.metrics.errors.Add(1)
		return next.ServeHTTP(w, r)
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	var d decision
	c := newCapture(w, "mf; fwd=uri-miss", func(status int, header http.Header) (string, bool) {
		d = decide(r, status, header, requested, h.now(), int(p.config.MaxObjectBytes))
		if d.detail != "" {
			if ready != nil {
				ready() // A private or uncacheable stream must not delay other requests.
			}
			return "detail=" + d.detail, false
		}
		return "detail=fill", true
	})
	c.limit = int(p.config.MaxObjectBytes)
	c.sink = file
	if err := next.ServeHTTP(c, r); err != nil {
		return err
	}
	c.finish()
	if !c.complete() {
		p.metrics.skipped.Add(1)
		return nil
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		p.metrics.errors.Add(1)
		return nil
	}
	// Publish complete small bodies independently of S3. This remains useful even when the
	// persistent quota is full or an upload fails; RAM keeps its own TTL and byte budget.
	if p.config.RAMBytes > 0 && c.copied <= p.config.RAMEntryBytes && c.copied <= int64(p.ram.maxEntry()) {
		body := make([]byte, int(c.copied))
		if _, err := io.ReadFull(file, body); err == nil {
			cached := d.entry(key, r.Header, body)
			cached.scope = h.Scope
			cached.size = cached.bytes()
			p.mu.Lock()
			publish := p.epoch == epoch && !p.invalid && cached.size <= int64(p.ram.maxEntry())
			if publish {
				p.ram.put(cached)
			}
			p.mu.Unlock()
			if publish && ready != nil {
				ready()
			}
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			p.metrics.errors.Add(1)
			return nil
		}
	}
	var random [24]byte
	if _, err = rand.Read(random[:]); err != nil {
		p.metrics.errors.Add(1)
		return nil
	}
	object := hex.EncodeToString(random[:])
	public := p.public != nil && p.cdnURL != "" && publicBody(r, d)
	if err := p.reserve(object, c.copied, public, h.now()); err != nil {
		p.metrics.skipped.Add(1)
		return nil
	}
	// Only a complete response reaches this point. A downstream proxy may close its stream as
	// soon as it has the body, so the upload belongs to the cache lifetime, not that request.
	// It remains synchronous, bounded by the existing fill slot, timeout and spool-file limits.
	ctx, cancel := context.WithTimeout(p.ctx, 2*time.Minute)
	defer cancel()
	if err := p.backend(public).Put(ctx, object, file, c.copied); err != nil {
		p.metrics.errors.Add(1)
		return nil
	}
	e := cachedResponse{Key: key, Scope: h.Scope, Status: d.status, Header: d.header, Vary: map[string]string{},
		Lifetime: d.lifetime, InitialAge: d.initialAge, Responded: d.responded, Object: object, Bytes: c.copied, Public: public}
	for _, name := range d.vary {
		e.Vary[name] = digest(fieldValue(r.Header, name))
	}
	if err := p.publish(e, epoch); err != nil {
		p.metrics.errors.Add(1)
		return nil
	}
	p.metrics.fills.Add(1)
	return nil
}

// Coalescing only delays requests; each waiter rechecks normal cache policy before reuse.
// Hash every request header because Vary is unknown until the origin responds. A purge starts
// a new generation, so post-purge requests never wait on an obsolete fill.
type cacheFlight struct {
	ready chan struct{}
	once  sync.Once
}

func (f *cacheFlight) release() { f.once.Do(func() { close(f.ready) }) }

func (h *Handler) coalesceFill(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler, key string, epoch uint64) error {
	p := h.persistent
	headers, _ := json.Marshal(r.Header)
	id := digest(key + "\x00" + strconv.FormatUint(epoch, 10) + "\x00" + string(headers))
	p.flightMu.Lock()
	flight := p.flights[id]
	if flight == nil && len(p.flights) < p.config.MaxConcurrentFills {
		flight = &cacheFlight{ready: make(chan struct{})}
		p.flights[id] = flight
		p.flightMu.Unlock()
		defer func() {
			p.flightMu.Lock()
			delete(p.flights, id)
			flight.release()
			p.flightMu.Unlock()
		}()
		// Another fill may have published between our first lookup and registration.
		return h.servePersistentAttempt(w, r, next, false, flight.release)
	}
	p.flightMu.Unlock()
	if flight != nil {
		select {
		case p.waiters <- struct{}{}:
			defer func() { <-p.waiters }()
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return r.Context().Err()
			case <-p.ctx.Done():
				return p.ctx.Err()
			case <-flight.ready:
			case <-timer.C:
			}
		default: // Bounded waiting; excess traffic follows the existing origin path.
		}
	}
	if err := r.Context().Err(); err != nil {
		return err
	}
	return h.servePersistentAttempt(w, r, next, false, nil)
}
