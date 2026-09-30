package mfcache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// start is the fake clock's first reading.
var start = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// harness is a handler on a fake clock in front of an origin that counts its calls.
type harness struct {
	t      *testing.T
	h      *Handler
	clock  time.Time
	calls  atomic.Int64
	origin func(w http.ResponseWriter, r *http.Request) error
}

// newHarness bounds the store at bound bytes; the origin answers with respond.
func newHarness(t *testing.T, bound int64, respond func(w http.ResponseWriter, r *http.Request) error) *harness {
	x := &harness{t: t, clock: start, origin: respond}
	x.h = &Handler{MaxBytes: bound, store: newStore(bound), now: func() time.Time { return x.clock }}
	return x
}

// fixed answers every request with status, the given headers, and a body naming the call.
func (x *harness) fixed(status int, header ...string) func(http.ResponseWriter, *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Add(header[i], header[i+1])
		}
		w.WriteHeader(status)
		if !bodyAllowed(r.Method, status) {
			return nil
		}
		_, err := fmt.Fprintf(w, "call %d", x.calls.Load())
		return err
	}
}

// bodyAllowed reports whether a response may carry content.
func bodyAllowed(method string, status int) bool {
	return method != http.MethodHead && status != http.StatusNoContent && status != http.StatusNotModified
}

// do sends a request with header pairs, after the edge set the deployment header.
func (x *harness) do(method, target string, header ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Add(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		x.calls.Add(1)
		return x.origin(w, r)
	})
	if err := x.h.ServeHTTP(w, r, next); err != nil {
		x.t.Fatalf("%s %s: %v", method, target, err)
	}
	return w
}

// status is the response's Cache-Status members from this cache.
func status(w *httptest.ResponseRecorder) string {
	var mine []string
	for _, v := range w.Header().Values("Cache-Status") {
		if strings.HasPrefix(v, "mf;") {
			mine = append(mine, v)
		}
	}
	return strings.Join(mine, ", ")
}

// expect fails unless the response carries Cache-Status member want.
func expect(t *testing.T, w *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := status(w); got != want {
		t.Fatalf("Cache-Status %q, want %q", got, want)
	}
}

func TestStoresOnlyWithExplicitFreshness(t *testing.T) {
	cases := []struct {
		name   string
		header []string
		want   string
	}{
		{"s-maxage", []string{"Cache-Control", "s-maxage=60"}, "stored"},
		{"max-age", []string{"Cache-Control", "max-age=60"}, "stored"},
		{"expires", []string{"Expires", start.Add(time.Minute).Format(http.TimeFormat)}, "stored"},
		{"no headers", nil, "detail=no-freshness"},
		{"public alone", []string{"Cache-Control", "public"}, "detail=no-freshness"},
		{"last-modified alone", []string{"Last-Modified", start.Add(-time.Hour).Format(http.TimeFormat)}, "detail=no-freshness"},
		{"max-age=0", []string{"Cache-Control", "max-age=0"}, "detail=expired"},
		{"invalid max-age", []string{"Cache-Control", "max-age=soon"}, "detail=expired"},
		{"invalid expires", []string{"Expires", "0"}, "detail=expired"},
		{"past expires", []string{"Expires", start.Add(-time.Minute).Format(http.TimeFormat)}, "detail=expired"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := newHarness(t, DefaultMaxBytes, nil)
			x.origin = x.fixed(200, c.header...)
			expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; "+c.want)
			second := x.do("GET", "/")
			if c.want == "stored" {
				expect(t, second, "mf; hit")
			} else if x.calls.Load() != 2 {
				t.Fatalf("origin called %d times, want 2", x.calls.Load())
			}
		})
	}
}

func TestSMaxAgeWinsOverMaxAgeAndExpires(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "max-age=600, s-maxage=10", "Expires", start.Add(time.Hour).Format(http.TimeFormat))
	x.do("GET", "/")
	x.clock = start.Add(9 * time.Second)
	expect(t, x.do("GET", "/"), "mf; hit")
	x.clock = start.Add(10 * time.Second)
	expect(t, x.do("GET", "/"), "mf; fwd=stale; stored")
}

func TestNeverStores(t *testing.T) {
	fresh := []string{"Cache-Control", "public, max-age=60"}
	cases := []struct {
		name    string
		method  string
		request []string
		status  int
		header  []string
		want    string
	}{
		{"private", "GET", nil, 200, []string{"Cache-Control", "private, max-age=60"}, "detail=private"},
		{"qualified private", "GET", nil, 200, []string{"Cache-Control", `max-age=60, private="X-User"`}, "detail=private"},
		{"no-store", "GET", nil, 200, []string{"Cache-Control", "no-store, max-age=60"}, "detail=no-store"},
		{"no-store with expires", "GET", nil, 200, []string{"Cache-Control", "no-store", "Expires", start.Add(time.Hour).Format(http.TimeFormat)}, "detail=no-store"},
		{"no-cache", "GET", nil, 200, []string{"Cache-Control", "no-cache, max-age=60"}, "detail=no-cache"},
		{"set-cookie", "GET", nil, 200, append([]string{"Set-Cookie", "s=1"}, fresh...), "detail=set-cookie"},
		{"authorization", "GET", []string{"Authorization", "Bearer u1"}, 200, []string{"Cache-Control", "max-age=60"}, "detail=authorization"},
		{"private with vary authorization", "GET", []string{"Authorization", "Bearer u1"}, 200, []string{"Cache-Control", "private, max-age=60", "Vary", "Authorization"}, "detail=private"},
		{"request no-store", "GET", []string{"Cache-Control", "no-store"}, 200, fresh, "detail=request-no-store"},
		{"vary star", "GET", nil, 200, append([]string{"Vary", "*"}, fresh...), "detail=vary-star"},
		{"206", "GET", nil, 206, fresh, "detail=status"},
		{"201", "GET", nil, 201, fresh, "detail=status"},
		{"302", "GET", nil, 302, fresh, "detail=status"},
		{"304", "GET", nil, 304, fresh, "detail=status"},
		{"500", "GET", nil, 500, fresh, "detail=status"},
		{"head", "HEAD", nil, 200, fresh, "detail=method"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := newHarness(t, DefaultMaxBytes, nil)
			x.origin = x.fixed(c.status, c.header...)
			expect(t, x.do(c.method, "/", c.request...), "mf; fwd=uri-miss; "+c.want)
			x.do("GET", "/", c.request...)
			if x.h.store.size != 0 && c.method == "GET" {
				t.Fatalf("stored %d bytes", x.h.store.size)
			}
			if c.method == "GET" && x.calls.Load() != 2 {
				t.Fatalf("origin called %d times, want 2", x.calls.Load())
			}
		})
	}
}

func TestUnsafeAndOtherMethodsPassThrough(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		x := newHarness(t, DefaultMaxBytes, nil)
		x.origin = x.fixed(200, "Cache-Control", "public, s-maxage=60")
		expect(t, x.do(method, "/"), "mf; fwd=method")
		expect(t, x.do(method, "/"), "mf; fwd=method")
		if x.calls.Load() != 2 || x.h.store.size != 0 {
			t.Fatalf("%s: calls %d, stored %d bytes", method, x.calls.Load(), x.h.store.size)
		}
	}
}

func TestAuthorizationStoresWithPublicOrSMaxage(t *testing.T) {
	for _, cc := range []string{"public, max-age=60", "s-maxage=60"} {
		x := newHarness(t, DefaultMaxBytes, nil)
		x.origin = x.fixed(200, "Cache-Control", cc)
		expect(t, x.do("GET", "/", "Authorization", "Bearer u1"), "mf; fwd=uri-miss; stored")
		expect(t, x.do("GET", "/"), "mf; hit")
	}
}

func TestStoresEveryHeuristicallyCacheableStatus(t *testing.T) {
	for _, code := range []int{200, 203, 204, 300, 301, 404, 405, 410, 414, 501} {
		x := newHarness(t, DefaultMaxBytes, nil)
		x.origin = x.fixed(code, "Cache-Control", "public, max-age=60")
		expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; stored")
		w := x.do("GET", "/")
		expect(t, w, "mf; hit")
		if w.Code != code {
			t.Fatalf("hit status %d, want %d", w.Code, code)
		}
	}
}

func TestHonoursVary(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Vary", "Accept-Encoding, accept-language")
		_, err := fmt.Fprintf(w, "%s|%s", r.Header.Get("Accept-Encoding"), r.Header.Get("Accept-Language"))
		return err
	}
	expect(t, x.do("GET", "/", "Accept-Encoding", "gzip"), "mf; fwd=uri-miss; stored")
	expect(t, x.do("GET", "/", "Accept-Encoding", "br"), "mf; fwd=vary-miss; stored")
	expect(t, x.do("GET", "/"), "mf; fwd=vary-miss; stored")
	expect(t, x.do("GET", "/", "Accept-Encoding", "gzip", "Accept-Language", "fr"), "mf; fwd=vary-miss; stored")
	for _, c := range []struct{ encoding, body string }{{"gzip", "gzip|"}, {"br", "br|"}, {"", "|"}} {
		var header []string
		if c.encoding != "" {
			header = []string{"Accept-Encoding", c.encoding}
		}
		w := x.do("GET", "/", header...)
		expect(t, w, "mf; hit")
		if w.Body.String() != c.body {
			t.Fatalf("Accept-Encoding %q served %q", c.encoding, w.Body.String())
		}
	}
	expect(t, x.do("GET", "/", "Accept-Encoding", "gzip ,  deflate"), "mf; fwd=vary-miss; stored")
	expect(t, x.do("GET", "/", "Accept-Encoding", "gzip, deflate"), "mf; hit")
	if x.calls.Load() != 5 {
		t.Fatalf("origin called %d times, want 5", x.calls.Load())
	}
}

func TestKeyIsHostPathQueryAndDeployment(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, s-maxage=60")
	expect(t, x.do("GET", "http://a.test/p?q=1"), "mf; fwd=uri-miss; stored")
	expect(t, x.do("GET", "http://A.TEST/p?q=1"), "mf; hit")
	for _, target := range []string{"http://b.test/p?q=1", "http://a.test/other?q=1", "http://a.test/p?q=2", "http://a.test/p"} {
		expect(t, x.do("GET", target), "mf; fwd=uri-miss; stored")
	}
	r := httptest.NewRequest("GET", "http://a.test/p?q=1", nil)
	r.Header.Set(DeploymentHeader, "app-r2-11111111")
	w := httptest.NewRecorder()
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error { return x.origin(w, r) })
	if err := x.h.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}
	expect(t, w, "mf; fwd=uri-miss; stored")
}

func TestAgeFollowsRFC9111(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=100", "Age", "30")
	miss := x.do("GET", "/")
	if miss.Header().Get("Age") != "30" {
		t.Fatalf("miss Age %q, want the origin's 30", miss.Header().Get("Age"))
	}
	x.clock = start.Add(20 * time.Second)
	hit := x.do("GET", "/")
	expect(t, hit, "mf; hit")
	if hit.Header().Get("Age") != "50" {
		t.Fatalf("hit Age %q, want 50", hit.Header().Get("Age"))
	}
	x.clock = start.Add(70 * time.Second)
	expect(t, x.do("GET", "/"), "mf; fwd=stale; stored")
}

func TestAgeCountsAnOldDate(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60", "Date", start.Add(-40*time.Second).Format(http.TimeFormat))
	x.do("GET", "/")
	hit := x.do("GET", "/")
	if hit.Header().Get("Age") != "40" {
		t.Fatalf("Age %q, want 40", hit.Header().Get("Age"))
	}
	x.clock = start.Add(20 * time.Second)
	expect(t, x.do("GET", "/"), "mf; fwd=stale; detail=expired")
}

func TestHitKeepsHeadersAndBodyAndServesHead(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60", "Content-Type", "text/plain", "Connection", "X-Hop", "X-Hop", "1")
	miss := x.do("GET", "/")
	hit := x.do("GET", "/")
	if hit.Body.String() != miss.Body.String() || hit.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("hit %q %v, miss %q", hit.Body.String(), hit.Header(), miss.Body.String())
	}
	if hit.Header().Get("X-Hop") != "" || hit.Header().Get("Date") == "" {
		t.Fatalf("hit header %v", hit.Header())
	}
	head := x.do("HEAD", "/")
	expect(t, head, "mf; hit")
	if head.Body.Len() != 0 || x.calls.Load() != 1 {
		t.Fatalf("HEAD body %q, origin calls %d", head.Body.String(), x.calls.Load())
	}
}

func TestCacheStatusFollowsUpstreamMembers(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60", "Cache-Status", "app; hit")
	miss := x.do("GET", "/")
	hit := x.do("GET", "/")
	for w, want := range map[*httptest.ResponseRecorder][]string{
		miss: {"app; hit", "mf; fwd=uri-miss; stored"},
		hit:  {"app; hit", "mf; hit"},
	} {
		if got := w.Header().Values("Cache-Status"); !slices.Equal(got, want) {
			t.Fatalf("Cache-Status %q, want %q", got, want)
		}
	}
}

func TestStoreIsBoundedInBytes(t *testing.T) {
	const bound = 64 << 10
	x := newHarness(t, bound, nil)
	body := strings.Repeat("x", 3000)
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=600")
		_, err := w.Write([]byte(body))
		return err
	}
	for i := range 100 {
		x.do("GET", fmt.Sprintf("/%d", i))
		if x.h.store.size > bound {
			t.Fatalf("store holds %d bytes over its bound %d", x.h.store.size, bound)
		}
	}
	expect(t, x.do("GET", "/99"), "mf; hit")
	expect(t, x.do("GET", "/0"), "mf; fwd=uri-miss; stored")
	held := len(x.h.store.byKey)
	if held < 10 || held > bound/3000 {
		t.Fatalf("store holds %d entries", held)
	}
}

func TestLeastRecentlyUsedIsEvictedFirst(t *testing.T) {
	x := newHarness(t, 64<<10, nil)
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=600")
		_, err := w.Write([]byte(strings.Repeat("x", 3000)))
		return err
	}
	x.do("GET", "/keep")
	for i := range 100 {
		x.do("GET", "/keep")
		x.do("GET", fmt.Sprintf("/%d", i))
	}
	expect(t, x.do("GET", "/keep"), "mf; hit")
}

func TestLargeResponsesAreNotStored(t *testing.T) {
	const bound = 64 << 10
	large := strings.Repeat("x", bound/entryShare+1)
	for _, length := range []bool{true, false} {
		x := newHarness(t, bound, nil)
		x.origin = func(w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("Cache-Control", "public, max-age=600")
			if length {
				w.Header().Set("Content-Length", fmt.Sprint(len(large)))
			}
			_, err := w.Write([]byte(large))
			return err
		}
		miss := x.do("GET", "/")
		if length {
			expect(t, miss, "mf; fwd=uri-miss; detail=too-large")
		}
		if miss.Body.String() != large {
			t.Fatal("the client did not get the whole body")
		}
		x.do("GET", "/")
		if x.calls.Load() != 2 || x.h.store.size != 0 {
			t.Fatalf("calls %d, stored %d bytes", x.calls.Load(), x.h.store.size)
		}
	}
}

func TestIncompleteResponsesAreNotStored(t *testing.T) {
	failures := map[string]func(w http.ResponseWriter, r *http.Request) error{
		"short body": func(w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("Cache-Control", "public, max-age=600")
			w.Header().Set("Content-Length", "10")
			_, err := w.Write([]byte("short"))
			return err
		},
		"handler error": func(w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("Cache-Control", "public, max-age=600")
			_, _ = w.Write([]byte("partial"))
			return errors.New("upstream went away")
		},
	}
	for name, origin := range failures {
		x := newHarness(t, DefaultMaxBytes, origin)
		r := httptest.NewRequest("GET", "/", nil)
		next := caddyhttp.HandlerFunc(origin)
		_ = x.h.ServeHTTP(httptest.NewRecorder(), r, next)
		if x.h.store.size != 0 {
			t.Fatalf("%s: stored %d bytes", name, x.h.store.size)
		}
	}
}

func TestConcurrentMissesAreNotCoalesced(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	var arrived sync.WaitGroup
	arrived.Add(2)
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		arrived.Done()
		arrived.Wait()
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, err := w.Write([]byte(r.Header.Get("X-User")))
		return err
	}
	var done sync.WaitGroup
	bodies := make([]string, 2)
	for i := range 2 {
		done.Add(1)
		go func() {
			defer done.Done()
			bodies[i] = x.do("GET", "/", "X-User", fmt.Sprint(i)).Body.String()
		}()
	}
	done.Wait()
	if x.calls.Load() != 2 || bodies[0] != "0" || bodies[1] != "1" {
		t.Fatalf("calls %d, bodies %q", x.calls.Load(), bodies)
	}
}

func TestUnsafeMethodInvalidatesTheURI(t *testing.T) {
	x := newHarness(t, DefaultMaxBytes, nil)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60")
	x.do("GET", "/doc")
	x.do("GET", "/other")
	x.do("POST", "/doc")
	expect(t, x.do("GET", "/doc"), "mf; fwd=uri-miss; stored")
	expect(t, x.do("GET", "/other"), "mf; hit")
}

func TestProvisionSharesOneBoundedStore(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	a, b, c := &Handler{}, &Handler{}, &Handler{MaxBytes: 1 << 20}
	for _, h := range []*Handler{a, b, c} {
		if err := h.Provision(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if a.MaxBytes != DefaultMaxBytes || a.store != b.store || a.store == c.store || a.store.max != DefaultMaxBytes {
		t.Fatalf("stores %p %p %p, bound %d", a.store, b.store, c.store, a.store.max)
	}
	for _, h := range []*Handler{a, b, c} {
		if err := h.Cleanup(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&Handler{MaxBytes: -1}).Provision(ctx); err == nil {
		t.Fatal("a negative bound was accepted")
	}
}

func TestModuleIsRegistered(t *testing.T) {
	info, err := caddy.GetModule("http.handlers.mf_cache")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := info.New().(caddyhttp.MiddlewareHandler); !ok {
		t.Fatal("mf_cache is not an HTTP handler")
	}
}
