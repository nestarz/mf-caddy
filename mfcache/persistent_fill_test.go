package mfcache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func waitForCache(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("cache operation did not reach expected state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPersistentRAMBeforeUpload(t *testing.T) {
	x, objects := persistentHarness(t, 32<<20)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	objects.beforePut = func() { close(entered); <-release }
	objects.failPut = true
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- x.do("GET", "/") }()
	<-entered
	response := x.do("GET", "/")
	expect(t, response, "mf; hit")
	if response.Body.String() != "call 1" || x.calls.Load() != 1 {
		t.Fatal("RAM waited for upload")
	}
	unblock()
	<-done
	expect(t, x.do("GET", "/"), "mf; hit")
	reopen(t, x, objects)
	objects.beforePut = nil
	objects.failPut = false
	expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; detail=fill")
}

func TestPersistentCoalescedFill(t *testing.T) {
	for _, ram := range []int64{0, 32 << 20} {
		t.Run(fmt.Sprint(ram), func(t *testing.T) {
			x, objects := persistentHarness(t, ram)
			originStarted, originRelease := make(chan struct{}), make(chan struct{})
			uploadStarted, uploadRelease := make(chan struct{}), make(chan struct{})
			var originOnce, uploadOnce sync.Once
			releaseOrigin := func() { originOnce.Do(func() { close(originRelease) }) }
			releaseUpload := func() { uploadOnce.Do(func() { close(uploadRelease) }) }
			defer releaseOrigin()
			defer releaseUpload()
			x.origin = func(w http.ResponseWriter, r *http.Request) error {
				close(originStarted)
				<-originRelease
				w.Header().Set("Cache-Control", "public, max-age=60")
				_, err := w.Write([]byte("complete"))
				return err
			}
			objects.beforePut = func() { close(uploadStarted); <-uploadRelease }
			leader := make(chan *httptest.ResponseRecorder, 1)
			go func() { leader <- x.do("GET", "/") }()
			<-originStarted
			const count = 8
			responses := make(chan *httptest.ResponseRecorder, count)
			for i := 0; i < count; i++ {
				go func() { responses <- x.do("GET", "/") }()
			}
			waitForCache(t, func() bool { return len(x.h.persistent.waiters) == count })
			releaseOrigin()
			<-uploadStarted
			if ram == 0 {
				releaseUpload()
			}
			for i := 0; i < count; i++ {
				select {
				case response := <-responses:
					if !strings.HasPrefix(status(response), "mf; hit") || response.Body.String() != "complete" {
						t.Fatal("waiter did not reuse complete body", status(response))
					}
				case <-time.After(time.Second):
					t.Fatal("waiter blocked behind upload despite RAM")
				}
			}
			releaseUpload()
			<-leader
			if x.calls.Load() != 1 {
				t.Fatal("duplicate origin calls", x.calls.Load())
			}
			if len(x.h.persistent.flights) != 0 || len(x.h.persistent.waiters) != 0 {
				t.Fatal("flight leaked")
			}
		})
	}
}

func TestPersistentCoalescingKeepsVariantsIndependent(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		variant := r.Header.Get("Cookie") + r.Header.Get("Authorization")
		if variant == "first" {
			close(started)
			<-release
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Vary", "Cookie, Authorization")
		_, err := w.Write([]byte(variant))
		return err
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- x.do("GET", "/", "Cookie", "first") }()
	<-started
	for _, header := range []string{"Cookie", "Authorization"} {
		response := x.do("GET", "/", header, "second")
		if response.Body.String() != "second" {
			t.Fatal("variants crossed")
		}
	}
	if err := x.h.persistent.invalidate(func(cachedResponse) bool { return true }); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	<-done
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	r.Header.Set("Cookie", "first")
	key := "app\x00" + primaryKey(r)
	entry, _, err := x.h.persistent.lookup(key, r.Header, x.clock)
	ram, _ := x.h.persistent.ram.lookup(key, r.Header, x.clock)
	if err != nil || entry != nil || ram != nil {
		t.Fatal("purged fill returned", err)
	}
}

func TestPersistentCancelledWaiterDoesNotStartOrigin(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		close(started)
		<-release
		return x.fixed(200, "Cache-Control", "public, max-age=60")(w, r)
	}
	leader := make(chan *httptest.ResponseRecorder, 1)
	go func() { leader <- x.do("GET", "/") }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	done := make(chan error, 1)
	go func() {
		done <- x.h.ServeHTTP(httptest.NewRecorder(), r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { t.Error("cancelled waiter reached origin"); return nil }))
	}()
	waitForCache(t, func() bool { return len(x.h.persistent.waiters) == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	once.Do(func() { close(release) })
	<-leader
	if x.calls.Load() != 1 {
		t.Fatal("duplicate origin")
	}
}

type brokenCacheWriter struct{ *httptest.ResponseRecorder }

func (w brokenCacheWriter) Write([]byte) (int, error) { return 0, errors.New("client disconnected") }

func TestPersistentBrokenClientNeverPublishes(t *testing.T) {
	x, objects := persistentHarness(t, 32<<20)
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, _ = w.Write([]byte("partial"))
		return nil
	})
	if err := x.h.ServeHTTP(brokenCacheWriter{httptest.NewRecorder()}, r, next); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(objects.dir)
	if len(files) != 0 {
		t.Fatal("partial body uploaded")
	}
	expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; detail=fill")
}

func TestPersistentLargeFillSkipsRAM(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	x.h.persistent.config.RAMEntryBytes = 1
	x.do("GET", "/")
	expect(t, x.do("GET", "/"), "mf; hit; detail=s3")
	expect(t, x.do("GET", "/"), "mf; hit; detail=s3")
}

func TestPersistentCoalescingWaitIsBounded(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		if x.calls.Load() == 1 {
			close(started)
			<-release
		}
		return x.fixed(200, "Cache-Control", "public, max-age=60")(w, r)
	}
	leader := make(chan *httptest.ResponseRecorder, 1)
	go func() { leader <- x.do("GET", "/") }()
	<-started
	began := time.Now()
	response := x.do("GET", "/")
	if response.Code != 200 || time.Since(began) > 3*time.Second || x.calls.Load() != 2 {
		t.Fatal("waiter did not fall back within its budget")
	}
	once.Do(func() { close(release) })
	<-leader
	if len(x.h.persistent.waiters) != 0 {
		t.Fatal("waiter permit leaked")
	}
}

func TestPersistentUncacheableHeadersReleaseWaiters(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	started, headers, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var headerOnce, finishOnce sync.Once
	defer headerOnce.Do(func() { close(headers) })
	defer finishOnce.Do(func() { close(finish) })
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		first := x.calls.Load() == 1
		if first {
			close(started)
			<-headers
		}
		w.Header().Set("Cache-Control", "private, no-store")
		w.WriteHeader(200)
		if first {
			<-finish
		}
		_, err := w.Write([]byte("private"))
		return err
	}
	leader := make(chan *httptest.ResponseRecorder, 1)
	go func() { leader <- x.do("GET", "/") }()
	<-started
	waiter := make(chan *httptest.ResponseRecorder, 1)
	go func() { waiter <- x.do("GET", "/") }()
	waitForCache(t, func() bool { return len(x.h.persistent.waiters) == 1 })
	headerOnce.Do(func() { close(headers) })
	select {
	case response := <-waiter:
		if response.Body.String() != "private" || x.calls.Load() != 2 {
			t.Fatal("private response was shared")
		}
	case <-time.After(time.Second):
		t.Fatal("private headers did not release waiter")
	}
	finishOnce.Do(func() { close(finish) })
	<-leader
}
