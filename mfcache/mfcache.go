// Package mfcache is the shared HTTP cache of the mf edge: a Caddy handler that stores only the
// responses their origin explicitly marks fresh and safe to share, in memory, bounded in bytes.
package mfcache

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	caddy.RegisterModule(Handler{})
}

// DefaultMaxBytes bounds a store whose handler leaves max_bytes unset: 256 MiB.
const DefaultMaxBytes = 256 << 20

// DeploymentHeader is the request header the edge sets on every route; its value is part of the key.
const DeploymentHeader = "X-Mf-Deployment"

// stores holds one store per bound, shared by every handler of the process configured with that
// bound, and kept across config reloads while any handler still uses it.
var stores = caddy.NewUsagePool()

// Handler implements the `http.handlers.mf_cache` module.
type Handler struct {
	// MaxBytes bounds the bytes the shared store holds: bodies, headers and keys. Default 256 MiB.
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// Persistent stores bodies in S3; MaxBytes remains the legacy memory-only option.
	Persistent *PersistentConfig `json:"persistent,omitempty"`
	// Scope isolates one app's entries across its hosts, deployments and Vary variants.
	Scope string `json:"scope,omitempty"`
	// PurgePath accepts authenticated tag purges. Only the SHA-256 of the bearer token is configured.
	PurgePath      string `json:"purge_path,omitempty"`
	PurgeTokenHash string `json:"purge_token_hash,omitempty"`

	store      *store
	persistent *persistentStore
	poolKey    string
	now        func() time.Time
}

// CaddyModule returns the Caddy module information.
func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mf_cache",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision attaches the handler to the store for its bound.
func (h *Handler) Provision(ctx caddy.Context) error {
	if err := h.validatePurge(); err != nil {
		return err
	}
	if h.Persistent != nil {
		if err := h.Persistent.defaults(); err != nil {
			return err
		}
		connections, identity, err := readConnections(h.Persistent.ConnectionFile)
		if err != nil {
			return err
		}
		h.poolKey = "persistent:" + h.Persistent.IndexPath
		value, _, err := stores.LoadOrNew(h.poolKey, func() (caddy.Destructor, error) {
			return openPersistent(*h.Persistent, identity, connections)
		})
		if err != nil {
			return err
		}
		p := value.(*persistentStore)
		if p.config != *h.Persistent || p.identity != identity {
			stores.Delete(h.poolKey)
			return fmt.Errorf("persistent cache configuration changed: restart Caddy to reopen its index")
		}
		h.persistent, h.now = p, time.Now
		if err := ctx.GetMetricsRegistry().Register(p.metrics); err != nil {
			var registered prometheus.AlreadyRegisteredError
			if !errors.As(err, &registered) {
				stores.Delete(h.poolKey)
				return err
			}
		}
		return nil
	}
	if h.MaxBytes < 0 {
		return fmt.Errorf("max_bytes must not be negative: %d", h.MaxBytes)
	}
	if h.MaxBytes == 0 {
		h.MaxBytes = DefaultMaxBytes
	}
	h.poolKey = strconv.FormatInt(h.MaxBytes, 10)
	value, _, err := stores.LoadOrNew(h.poolKey, func() (caddy.Destructor, error) {
		return newStore(h.MaxBytes), nil
	})
	if err != nil {
		return err
	}
	h.store = value.(*store)
	h.now = time.Now
	return nil
}

// Cleanup releases the handler's use of its store.
func (h *Handler) Cleanup() error {
	_, err := stores.Delete(h.poolKey)
	return err
}

// ServeHTTP answers a fresh matching entry from the store, and otherwise forwards the request and
// stores the response when the contract allows it. Persistent mode coalesces matching misses.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if h.PurgePath != "" && r.URL.Path == h.PurgePath {
		return h.purge(w, r)
	}
	if r.Header.Get("Upgrade") != "" {
		return next.ServeHTTP(w, r)
	}
	if h.persistent != nil {
		return h.servePersistent(w, r, next)
	}
	key := h.Scope + "\x00" + primaryKey(r)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		c := newCapture(w, "mf; fwd=method", func(int, http.Header) (string, bool) { return "", false })
		if err := next.ServeHTTP(c, r); err != nil {
			return err
		}
		c.finish()
		if unsafeMethod(r.Method) && c.status >= 200 && c.status < 400 {
			h.store.invalidate(key)
		}
		return nil
	}
	epoch := h.store.generation()
	requested := h.now()
	e, fwd := h.store.lookup(key, r.Header, requested)
	if e != nil && reusable(r, e) && !has(directives(r.Header.Values("Cache-Control")), "no-store") {
		serve(w, r, e, requested)
		return nil
	}
	var d decision
	c := newCapture(w, "mf; fwd="+fwd, func(status int, header http.Header) (string, bool) {
		d = decide(r, status, header, requested, h.now(), h.store.maxEntry())
		if d.detail != "" {
			return "detail=" + d.detail, false
		}
		return "stored", true
	})
	c.limit = h.store.maxEntry()
	if err := next.ServeHTTP(c, r); err != nil {
		return err
	}
	c.finish()
	if d.detail == "" && c.complete() {
		e := d.entry(key, r.Header, c.body.Bytes())
		e.scope = h.Scope
		e.size += int64(len(h.Scope))
		h.store.putAt(e, epoch)
	}
	return nil
}

// serve writes a stored response with its current Age and a hit member in Cache-Status.
func serve(w http.ResponseWriter, r *http.Request, e *entry, now time.Time) {
	header := w.Header()
	for name, values := range e.header {
		header[name] = append([]string(nil), values...)
	}
	header.Set("Age", strconv.FormatInt(int64(e.age(now)/time.Second), 10))
	header.Add("Cache-Status", "mf; hit")
	w.WriteHeader(e.status)
	if r.Method != http.MethodHead && len(e.body) > 0 {
		_, _ = w.Write(e.body)
	}
}

// primaryKey is the host, the path and query, and the deployment the edge routed the request to.
func primaryKey(r *http.Request) string {
	return strings.ToLower(r.Host) + "\x00" + r.URL.RequestURI() + "\x00" + r.Header.Get(DeploymentHeader)
}

// unsafeMethod reports a method whose success invalidates the stored responses for its URI.
func unsafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	}
	return true
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
)
