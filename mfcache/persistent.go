package mfcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// PersistentConfig separates stored-body capacity from optional RAM acceleration. Zero RAMBytes
// disables RAM bodies. Every handler on one host shares this index, budgets and upload admission.
type PersistentConfig struct {
	ConnectionFile     string `json:"connection_file"`
	IndexPath          string `json:"index_path"`
	RAMBytes           int64  `json:"ram_bytes"`
	RAMEntryBytes      int64  `json:"ram_entry_bytes,omitempty"`
	StreamBufferBytes  int    `json:"stream_buffer_bytes,omitempty"`
	MaxObjectBytes     int64  `json:"max_object_bytes,omitempty"`
	MaxStoreBytes      int64  `json:"max_store_bytes,omitempty"`
	MaxConcurrentReads int    `json:"max_concurrent_reads,omitempty"`
	MaxConcurrentFills int    `json:"max_concurrent_fills,omitempty"`
}

func (c *PersistentConfig) defaults() error {
	if c.RAMEntryBytes == 0 {
		c.RAMEntryBytes = 128 << 10
	}
	if c.StreamBufferBytes == 0 {
		c.StreamBufferBytes = 64 << 10
	}
	if c.MaxObjectBytes == 0 {
		c.MaxObjectBytes = 64 << 20
	}
	if c.MaxStoreBytes == 0 {
		c.MaxStoreBytes = 2 << 30
	}
	if c.MaxConcurrentReads == 0 {
		c.MaxConcurrentReads = 64
	}
	if c.MaxConcurrentFills == 0 {
		c.MaxConcurrentFills = 4
	}
	if !filepath.IsAbs(c.IndexPath) || c.ConnectionFile == "" || c.RAMBytes < 0 ||
		c.RAMEntryBytes < 1 || c.StreamBufferBytes < 4096 || c.StreamBufferBytes > 1<<20 ||
		c.MaxObjectBytes < 1 || c.MaxObjectBytes > 1<<30 || c.MaxStoreBytes < c.MaxObjectBytes ||
		c.MaxConcurrentReads < 1 || c.MaxConcurrentReads > 1024 || c.MaxConcurrentFills < 1 || c.MaxConcurrentFills > 32 {
		return errors.New("invalid persistent cache limits or paths")
	}
	return nil
}

// The private index stores no bodies. Hash Vary values so cookies and authorization tokens are
// never persisted as variant metadata. Object names are random and never reused after invalidation.
type cachedResponse struct {
	Key        string
	Scope      string
	Status     int
	Header     http.Header
	Vary       map[string]string
	Lifetime   time.Duration
	InitialAge time.Duration
	Responded  time.Time
	Object     string
	Bytes      int64
	Public     bool
}

func (e cachedResponse) entry() *entry {
	return &entry{key: e.Key, scope: e.Scope, status: e.Status, header: e.Header, lifetime: e.Lifetime,
		initialAge: e.InitialAge, responded: e.Responded}
}

func (e cachedResponse) matches(header http.Header) bool {
	for name, value := range e.Vary {
		if digest(fieldValue(header, name)) != value {
			return false
		}
	}
	return true
}

func (e cachedResponse) id() string {
	vary, _ := json.Marshal(e.Vary)
	return digest(e.Key) + "/" + digest(string(vary))
}

func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

type storedObject struct {
	Bytes       int64
	Public      bool
	DeleteAfter time.Time
}

var responsesBucket = []byte("responses-v1")
var objectsBucket = []byte("objects-v1")

// The index is private to this host. The existing MF purge coordinator contacts every ingress.
// A purge commits index removal before acknowledgement; the mutex also fences RAM promotions and
// fills. After a process restart no old fill remains, and removed index entries cannot return.
type persistentStore struct {
	config          PersistentConfig
	identity        string
	db              *bolt.DB
	private, public ObjectStore
	cdnURL          string
	cdnClient       *http.Client
	ram             *store
	mu              sync.RWMutex
	epoch           uint64
	invalid         bool
	reads, fills    chan struct{}
	flightMu        sync.Mutex
	flights         map[string]*cacheFlight
	waiters         chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	uploadMu        sync.Mutex
	uploadWG        sync.WaitGroup
	closing         bool
	metrics         *cacheMetrics
	orphanCursor    [2]string
	orphanNext      [2]time.Time
}

func openPersistent(c PersistentConfig, identity string, connections cacheConnections) (*persistentStore, error) {
	private, err := newS3Objects(connections.Private)
	if err != nil {
		return nil, err
	}
	var public ObjectStore
	if connections.Public != nil {
		if connections.Public.Endpoint == connections.Private.Endpoint && connections.Public.Bucket == connections.Private.Bucket {
			return nil, errors.New("public cache bodies require a separate bucket")
		}
		public, err = newS3Objects(*connections.Public)
		if err != nil {
			return nil, err
		}
	}
	base := connections.PublicReadURL
	if base != "" {
		u, err := url.Parse(base)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || public == nil {
			return nil, errors.New("public CDN URL requires HTTPS and a separate public body store")
		}
		base = strings.TrimSuffix(base, "/") + "/"
	}
	p, err := newPersistent(c, private, public)
	if err != nil {
		return nil, err
	}
	// A reused index must never map entries onto a different bucket or namespace.
	locations := []string{connections.Private.Endpoint, connections.Private.Bucket, connections.Private.Prefix}
	if connections.Public != nil {
		locations = append(locations, connections.Public.Endpoint, connections.Public.Bucket, connections.Public.Prefix)
	}
	locationJSON, _ := json.Marshal(locations)
	err = p.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("settings-v1"))
		if err != nil {
			return err
		}
		previous := b.Get([]byte("location"))
		if previous != nil && string(previous) != string(locationJSON) {
			return errors.New("cache storage location changed: use a separate index path")
		}
		return b.Put([]byte("location"), locationJSON)
	})
	if err != nil {
		p.Destruct()
		return nil, err
	}
	p.identity, p.cdnURL = identity, base
	p.cdnClient, _ = objectHTTPClient("")
	p.startMaintenance()
	return p, nil
}

func newPersistent(c PersistentConfig, private, public ObjectStore) (*persistentStore, error) {
	if err := c.defaults(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(c.IndexPath), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(c.IndexPath, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open cache index: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &persistentStore{config: c, db: db, private: private, public: public, ram: newStore(c.RAMBytes),
		reads: make(chan struct{}, c.MaxConcurrentReads), fills: make(chan struct{}, c.MaxConcurrentFills), ctx: ctx, cancel: cancel, metrics: newCacheMetrics(),
		flights: make(map[string]*cacheFlight), waiters: make(chan struct{}, c.MaxConcurrentReads)}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{responsesBucket, objectsBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		cancel()
		db.Close()
		return nil, err
	}
	// Private spools are disposable, and the DB lock ensures another Caddy cannot be using them.
	files, err := filepath.Glob(c.IndexPath + ".fill-*")
	if err != nil {
		p.Destruct()
		return nil, err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			p.Destruct()
			return nil, err
		}
	}
	return p, nil
}

func (p *persistentStore) startMaintenance() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		timer := time.NewTicker(30 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case now := <-timer.C:
				p.collect(now)
				p.collectOrphans(now)
			}
		}
	}()
}

func (p *persistentStore) startUpload(upload func()) bool {
	p.uploadMu.Lock()
	defer p.uploadMu.Unlock()
	if p.closing {
		return false
	}
	p.uploadWG.Add(1)
	go func() { defer p.uploadWG.Done(); upload() }()
	return true
}
func (p *persistentStore) Destruct() error {
	p.uploadMu.Lock()
	p.closing = true
	p.cancel()
	p.uploadMu.Unlock()
	p.uploadWG.Wait()
	p.wg.Wait()
	return p.db.Close()
}

// S3-compatible LIST recovers abandoned bodies when the local index is lost. The dedicated
// host prefix and a 24-hour grace period exclude active uploads and every artifact object.
func (p *persistentStore) collectOrphans(now time.Time) {
	for i, backend := range []ObjectStore{p.private, p.public} {
		if now.Before(p.orphanNext[i]) {
			continue
		}
		lister, ok := backend.(objectLister)
		if !ok {
			continue
		}
		ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		objects, next, err := lister.List(ctx, p.orphanCursor[i])
		cancel()
		if err != nil {
			p.metrics.errors.Add(1)
			continue
		}
		p.orphanCursor[i] = next
		if next == "" {
			p.orphanNext[i] = now.Add(24 * time.Hour)
		}
		for _, object := range objects {
			if now.Sub(object.Modified) < 24*time.Hour {
				continue
			}
			known := false
			if err := p.db.View(func(tx *bolt.Tx) error { known = tx.Bucket(objectsBucket).Get([]byte(object.Key)) != nil; return nil }); err != nil {
				p.metrics.errors.Add(1)
				continue
			}
			if known {
				continue
			}
			ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
			err := backend.Delete(ctx, object.Key)
			cancel()
			if err != nil {
				p.metrics.errors.Add(1)
			} else {
				p.metrics.deleted.Add(1)
			}
		}
	}
}

func (p *persistentStore) backend(public bool) ObjectStore {
	if public {
		return p.public
	}
	return p.private
}

func (p *persistentStore) lookup(key string, header http.Header, now time.Time) (*cachedResponse, uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.invalid {
		return nil, p.epoch, errors.New("cache index awaits successful invalidation")
	}
	var found *cachedResponse
	err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(responsesBucket).Cursor()
		prefix := digest(key) + "/"
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var e cachedResponse
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			if e.entry().fresh(now) && e.matches(header) {
				found = &e
				break
			}
		}
		return nil
	})
	return found, p.epoch, err
}

func tagged(e cachedResponse, scope string, tags map[string]bool) bool {
	if e.Scope != scope {
		return false
	}
	for _, line := range e.Header.Values("Cache-Tag") {
		for _, tag := range strings.Split(line, ",") {
			if tags[strings.TrimSpace(tag)] {
				return true
			}
		}
	}
	return false
}

// invalidate is the durable visibility boundary, independent of when S3 deletes finish.
func (p *persistentStore) invalidate(match func(cachedResponse) bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(responsesBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e cachedResponse
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			if match(e) {
				if err := retire(tx, e.Object); err != nil {
					return err
				}
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	// On a disk error clear RAM as well, refuse the purge, and stop serving index entries until
	// the caller retries successfully. A half-persisted invalidation must never report success.
	if err != nil {
		p.invalid = true
		p.epoch++
		p.ram.Destruct()
		p.metrics.errors.Add(1)
		return err
	}
	p.invalid = false
	p.epoch++
	p.ram.Destruct() // Small shared RAM tier; unrelated durable entries remain available.
	p.metrics.purges.Add(1)
	return nil
}

func retire(tx *bolt.Tx, key string) error {
	b := tx.Bucket(objectsBucket)
	v := b.Get([]byte(key))
	if v == nil {
		return nil
	}
	var obj storedObject
	if err := json.Unmarshal(v, &obj); err != nil {
		return err
	}
	obj.DeleteAfter = time.Unix(0, 0)
	data, _ := json.Marshal(obj)
	return b.Put([]byte(key), data)
}

// reserve records an upload before it starts. A crash between PUT and publication leaves a tracked
// object which maintenance will remove. Failed deletes still occupy the budget.
func (p *persistentStore) reserve(key string, size int64, public bool, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		var used int64
		count := 0
		err := b.ForEach(func(_, v []byte) error {
			var o storedObject
			if err := json.Unmarshal(v, &o); err != nil {
				return err
			}
			used += o.Bytes
			count++
			return nil
		})
		if err != nil {
			return err
		}
		if used+size > p.config.MaxStoreBytes || count >= 8192 {
			return errors.New("persistent cache budget exhausted")
		}
		data, _ := json.Marshal(storedObject{Bytes: size, Public: public, DeleteAfter: now.Add(time.Hour)})
		return b.Put([]byte(key), data)
	})
}

func (p *persistentStore) publish(e cachedResponse, epoch uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db.Update(func(tx *bolt.Tx) error {
		if epoch != p.epoch || p.invalid {
			return retire(tx, e.Object)
		}
		b := tx.Bucket(responsesBucket)
		id := []byte(e.id())
		if old := b.Get(id); old != nil {
			var previous cachedResponse
			if err := json.Unmarshal(old, &previous); err != nil {
				return err
			}
			if err := retire(tx, previous.Object); err != nil {
				return err
			}
		}
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if len(data) > 16<<10 {
			return retire(tx, e.Object)
		}
		if err := b.Put(id, data); err != nil {
			return err
		}
		obj, _ := json.Marshal(storedObject{Bytes: e.Bytes, Public: e.Public, DeleteAfter: e.Responded.Add(e.Lifetime - e.InitialAge)})
		return tx.Bucket(objectsBucket).Put([]byte(e.Object), obj)
	})
}

// collect expires entries, evicts the oldest entries when nearly full, and deletes bounded batches.
// No requests wait for S3 deletion; inability to reclaim makes new fills skip instead of overgrow.
func (p *persistentStore) collect(now time.Time) {
	p.mu.Lock()
	var garbage = make(map[string]storedObject)
	err := p.db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket(objectsBucket)
		var used int64
		count := 0
		if err := objects.ForEach(func(k, v []byte) error {
			var o storedObject
			if err := json.Unmarshal(v, &o); err != nil {
				return err
			}
			if now.Before(o.DeleteAfter) {
				used += o.Bytes
				count++
			}
			if !now.Before(o.DeleteAfter) && len(garbage) < 64 {
				garbage[string(k)] = o
			}
			return nil
		}); err != nil {
			return err
		}
		responses := tx.Bucket(responsesBucket)
		c := responses.Cursor()
		var oldest *cachedResponse
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e cachedResponse
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			if !e.entry().fresh(now) {
				if err := retire(tx, e.Object); err != nil {
					return err
				}
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}
			if oldest == nil || e.Responded.Before(oldest.Responded) {
				copy := e
				oldest = &copy
			}
		}
		if oldest != nil && (used > p.config.MaxStoreBytes-p.config.MaxObjectBytes || count >= 8000) {
			if err := retire(tx, oldest.Object); err != nil {
				return err
			}
			if err := responses.Delete([]byte(oldest.id())); err != nil {
				return err
			}
			p.ram.invalidate(oldest.Key)
			p.metrics.evictions.Add(1)
		}
		return nil
	})
	p.mu.Unlock()
	if err != nil {
		p.metrics.errors.Add(1)
		return
	}
	for key, obj := range garbage {
		ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		err := p.backend(obj.Public).Delete(ctx, key)
		cancel()
		if err != nil {
			p.metrics.errors.Add(1)
			continue
		}
		if err := p.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(objectsBucket).Delete([]byte(key)) }); err != nil {
			p.metrics.errors.Add(1)
		} else {
			p.metrics.deleted.Add(1)
		}
	}
}
