package mfcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// Bodies on disk keep the fixture itself from hiding an implementation's body-sized allocations.
type fileObjects struct {
	dir                 string
	failPut, failDelete bool
	beforePut           func()
	open                func(context.Context, string) (io.ReadCloser, int64, error)
}

type cancellationAwareObjects struct{ *fileObjects }

func (s cancellationAwareObjects) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.fileObjects.Put(ctx, key, body, size)
}

func TestPersistentCompletedResponseSurvivesClientCancellation(t *testing.T) {
	x, objects := persistentHarness(t, 0)
	x.h.persistent.private = cancellationAwareObjects{objects}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	w := httptest.NewRecorder()
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		x.calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Length", "8")
		_, err := w.Write([]byte("complete"))
		cancel() // A downstream proxy has the complete response and closes its origin stream.
		return err
	})
	if err := x.h.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60")
	response := x.do("GET", "/")
	expect(t, response, "mf; hit; detail=s3")
	if response.Body.String() != "complete" || x.calls.Load() != 1 {
		t.Fatal("completed fill was lost after downstream cancellation")
	}
}

func TestPersistentBudgetAndAdmission(t *testing.T) {
	x, objects := persistentHarness(t, 0)
	p := x.h.persistent
	p.config.MaxObjectBytes = 8
	p.config.MaxStoreBytes = 8
	x.do("GET", "/a")
	x.do("GET", "/b")
	files, _ := os.ReadDir(objects.dir)
	if len(files) != 1 {
		t.Fatal("storage quota exceeded")
	}
	objects.failDelete = true
	p.collect(x.clock.Add(time.Hour))
	p.collect(x.clock.Add(time.Hour))
	x.do("GET", "/c")
	files, _ = os.ReadDir(objects.dir)
	if len(files) != 1 {
		t.Fatal("failed deletion released budget")
	}
	objects.failDelete = false
	p.collect(x.clock.Add(time.Hour))
	x.do("GET", "/d")
	for i := 0; i < cap(p.reads); i++ {
		p.reads <- struct{}{}
	}
	r := x.do("GET", "/d")
	if r.Code != 503 || x.calls.Load() != 4 {
		t.Fatal("read saturation woke origin", r.Code, x.calls.Load())
	}
	for len(p.reads) > 0 {
		<-p.reads
	}
	for i := 0; i < cap(p.fills); i++ {
		p.fills <- struct{}{}
	}
	r = x.do("GET", "/uncached")
	if r.Code != 200 {
		t.Fatal("fill saturation broke response")
	}
	for len(p.fills) > 0 {
		<-p.fills
	}
}

func TestPersistentInterruptedOriginNeverPublishes(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	x.origin = func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Length", "100")
		_, err := w.Write([]byte("truncated"))
		return err
	}
	x.do("GET", "/")
	x.do("GET", "/")
	if x.calls.Load() != 2 {
		t.Fatal("truncated response was cached")
	}
}

func TestPersistentPrivateAuthorizationCannotReuseAnonymousEntry(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	x.origin = x.fixed(200, "Cache-Control", "max-age=600")
	x.do("GET", "/")
	x.do("GET", "/")
	x.do("GET", "/", "Authorization", "Bearer secret")
	if x.calls.Load() != 2 {
		t.Fatal("authorization reused unapproved shared entry")
	}
}

// Opt-in real S3 protocol coverage uses the same local RustFS executable MF's integrations pin.
func TestPersistentRustFS(t *testing.T) {
	binary := os.Getenv("MF_TEST_RUSTFS")
	if binary == "" {
		t.Skip("set MF_TEST_RUSTFS to run the real S3 integration")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	cmd := exec.Command(binary, "server")
	cmd.Env = append(os.Environ(), "RUSTFS_ACCESS_KEY=testaccesskey", "RUSTFS_SECRET_KEY=testsecretkey123", "RUSTFS_ADDRESS="+address, "RUSTFS_VOLUMES="+t.TempDir(), "RUSTFS_CONSOLE_ENABLE=false")
	log, err := os.CreateTemp(t.TempDir(), "rustfs")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	store, err := newS3Objects(S3Connection{Endpoint: "http://" + address, Bucket: "mf-cache-test", Prefix: "responses", Region: "us-east-1", PathStyle: true, AccessKey: "testaccesskey", SecretKey: "testsecretkey123"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		_, err = store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("mf-cache-test")})
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	p, err := newPersistent(PersistentConfig{IndexPath: filepath.Join(t.TempDir(), "index"), ConnectionFile: "test"}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Destruct()
	x := newHarness(t, 0, nil)
	x.h.persistent = p
	x.h.Scope = "app"
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=600")
	x.do("GET", "/")
	r := x.do("GET", "/")
	expect(t, r, "mf; hit; detail=s3")
	if r.Body.String() != "call 1" {
		t.Fatal(r.Body.String())
	}
	listed, _, err := store.List(context.Background(), "")
	if err != nil || len(listed) != 1 {
		t.Fatal("real S3 listing", len(listed), err)
	}
	if err := p.invalidate(func(cachedResponse) bool { return true }); err != nil {
		t.Fatal(err)
	}
	p.collect(x.clock)
	listed, _, err = store.List(context.Background(), "")
	if err != nil || len(listed) != 0 {
		t.Fatal("real S3 deletion", len(listed), err)
	}
}

func TestPersistentProvisionSharesStoreAndRegistersMetrics(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	dir := t.TempDir()
	credential := filepath.Join(dir, "credential.json")
	data, _ := json.Marshal(cacheConnections{Private: S3Connection{Endpoint: "https://s3.example.com", Bucket: "cache", Prefix: "test", Region: "auto", AccessKey: "key", SecretKey: "secret"}})
	if err := os.WriteFile(credential, data, 0600); err != nil {
		t.Fatal(err)
	}
	config := PersistentConfig{ConnectionFile: credential, IndexPath: filepath.Join(dir, "index.db"), RAMBytes: 32 << 20}
	a, b := &Handler{Persistent: &config}, &Handler{Persistent: &config}
	if err := a.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Cleanup()
	if err := b.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Cleanup()
	if a.persistent != b.persistent {
		t.Fatal("budgets are not shared")
	}
	if _, err := ctx.GetMetricsRegistry().Gather(); err != nil {
		t.Fatal(err)
	}
}

func (f *fileObjects) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if f.open != nil {
		return f.open(ctx, key)
	}
	b, err := os.Open(filepath.Join(f.dir, key))
	if err != nil {
		return nil, 0, err
	}
	s, err := b.Stat()
	if err != nil {
		b.Close()
		return nil, 0, err
	}
	return b, s.Size(), nil
}
func (f *fileObjects) Put(_ context.Context, key string, body io.ReadSeeker, size int64) error {
	if f.beforePut != nil {
		f.beforePut()
	}
	if f.failPut {
		return errors.New("PUT unavailable")
	}
	w, err := os.Create(filepath.Join(f.dir, key))
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, body)
	return err
}
func (f *fileObjects) Delete(_ context.Context, key string) error {
	if f.failDelete {
		return errors.New("DELETE unavailable")
	}
	err := os.Remove(filepath.Join(f.dir, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func persistentHarness(t *testing.T, ram int64) (*harness, *fileObjects) {
	t.Helper()
	objects := &fileObjects{dir: t.TempDir()}
	x := newHarness(t, 0, nil)
	p, err := newPersistent(PersistentConfig{IndexPath: filepath.Join(t.TempDir(), "index"), ConnectionFile: "test", RAMBytes: ram}, objects, nil)
	if err != nil {
		t.Fatal(err)
	}
	x.h.persistent = p
	x.h.Scope = "app"
	t.Cleanup(func() { x.h.persistent.Destruct() })
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=600", "Cache-Tag", "page")
	return x, objects
}

func reopen(t *testing.T, x *harness, objects *fileObjects) {
	t.Helper()
	old := x.h.persistent
	c := old.config
	if err := old.Destruct(); err != nil {
		t.Fatal(err)
	}
	p, err := newPersistent(c, objects, old.public)
	if err != nil {
		t.Fatal(err)
	}
	x.h.persistent = p
}

func TestPersistentRestartPurgeAndRAM(t *testing.T) {
	for _, ram := range []int64{0, 32 << 20} {
		t.Run(fmt.Sprint(ram), func(t *testing.T) {
			x, objects := persistentHarness(t, ram)
			expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; detail=fill")
			if ram > 0 {
				expect(t, x.do("GET", "/"), "mf; hit")
			} else {
				expect(t, x.do("GET", "/"), "mf; hit; detail=s3")
			}
			reopen(t, x, objects)
			x.clock = x.clock.Add(10 * time.Second)
			r := x.do("GET", "/")
			expect(t, r, "mf; hit; detail=s3")
			if r.Header().Get("Age") != "10" {
				t.Fatal(r.Header())
			}
			if x.calls.Load() != 1 {
				t.Fatal("restart woke origin")
			}
			if err := x.h.persistent.invalidate(func(e cachedResponse) bool { return tagged(e, "app", map[string]bool{"page": true}) }); err != nil {
				t.Fatal(err)
			}
			reopen(t, x, objects)
			expect(t, x.do("GET", "/"), "mf; fwd=uri-miss; detail=fill")
			if x.calls.Load() != 2 {
				t.Fatal("purge resurrected entry")
			}
		})
	}
}

func TestPersistentVariantsIsolationAndPrivateResponses(t *testing.T) {
	x, _ := persistentHarness(t, 32<<20)
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60", "Vary", "Cookie")
	first := x.do("GET", "/", "Cookie", "consent=yes").Body.String()
	second := x.do("GET", "/", "Cookie", "consent=no").Body.String()
	if first == second {
		t.Fatal("variants mixed")
	}
	if x.do("GET", "/", "Cookie", "consent=yes").Body.String() != first {
		t.Fatal("wrong variant")
	}
	for _, header := range [][]string{{"Cache-Control", "private, max-age=60"}, {"Cache-Control", "no-store, max-age=60"}, {"Cache-Control", "public, max-age=60", "Set-Cookie", "session=secret"}} {
		x.origin = x.fixed(200, header...)
		before := x.calls.Load()
		x.do("GET", "/private")
		x.do("GET", "/private")
		if x.calls.Load() != before+2 {
			t.Fatal("stored private response")
		}
	}
	before := x.calls.Load()
	x.do("GET", "/", "Cookie", "consent=yes", "Cache-Control", "no-store")
	if x.calls.Load() != before+1 {
		t.Fatal("request no-store reused cache")
	}
}

func TestPersistentPurgeFencesInFlightFill(t *testing.T) {
	x, objects := persistentHarness(t, 32<<20)
	objects.beforePut = func() {
		if err := x.h.persistent.invalidate(func(e cachedResponse) bool { return true }); err != nil {
			t.Error(err)
		}
	}
	x.do("GET", "/")
	objects.beforePut = nil
	x.do("GET", "/")
	if x.calls.Load() != 2 {
		t.Fatal("pre-purge fill returned")
	}
	x.h.persistent.collect(x.clock.Add(2 * time.Hour))
	files, _ := os.ReadDir(objects.dir)
	if len(files) != 0 {
		t.Fatal("expired or fenced objects retained", len(files))
	}
}

func TestPersistentFailedAndOversizedFills(t *testing.T) {
	x, objects := persistentHarness(t, 0)
	objects.failPut = true
	x.do("GET", "/")
	objects.failPut = false
	x.do("GET", "/")
	if x.calls.Load() != 2 {
		t.Fatal("failed PUT published")
	}
	x.h.persistent.config.MaxObjectBytes = 4
	x.origin = x.fixed(200, "Cache-Control", "public, max-age=60")
	x.do("GET", "/large")
	x.do("GET", "/large")
	if x.calls.Load() != 4 {
		t.Fatal("oversized fill published")
	}
	spools, _ := filepath.Glob(x.h.persistent.config.IndexPath + ".fill-*")
	if len(spools) != 0 {
		t.Fatal("spool leak")
	}
}

type gateReader struct {
	remaining int64
	wrote     <-chan struct{}
	first     bool
	max       int
}

func (g *gateReader) Read(b []byte) (int, error) {
	if g.remaining == 0 {
		return 0, io.EOF
	}
	if len(b) > g.max {
		return 0, errors.New("unbounded read buffer")
	}
	if g.first {
		select {
		case <-g.wrote:
		case <-time.After(time.Second):
			return 0, errors.New("body buffered before delivery")
		}
	}
	g.first = true
	n := int(min(int64(len(b)), g.remaining))
	clear(b[:n])
	g.remaining -= int64(n)
	return n, nil
}
func (*gateReader) Close() error { return nil }

type countingWriter struct {
	header http.Header
	bytes  int64
	first  chan struct{}
	once   sync.Once
}

func (w *countingWriter) Header() http.Header { return w.header }
func (*countingWriter) WriteHeader(int)       {}
func (w *countingWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.first) })
	w.bytes += int64(len(b))
	return len(b), nil
}

func TestPersistentLargeHitStreamsBeforeTail(t *testing.T) {
	x, objects := persistentHarness(t, 32<<20)
	size := int64(512 << 20)
	r := httptest.NewRequest("GET", "/large", nil)
	r.Header.Set(DeploymentHeader, "app-r1-00000000")
	e := cachedResponse{Key: "app\x00" + primaryKey(r), Scope: "app", Status: 200, Header: http.Header{"Cache-Control": {"public, max-age=600"}}, Lifetime: 600 * time.Second, Responded: x.clock, Object: "large", Bytes: size}
	if err := x.h.persistent.reserve(e.Object, size, false, x.clock); err != nil {
		t.Fatal(err)
	}
	if err := x.h.persistent.publish(e, 0); err != nil {
		t.Fatal(err)
	}
	w := &countingWriter{header: http.Header{}, first: make(chan struct{})}
	objects.open = func(context.Context, string) (io.ReadCloser, int64, error) {
		return &gateReader{remaining: size, wrote: w.first, max: 64 << 10}, size, nil
	}
	if err := x.h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { t.Fatal("woke origin"); return nil })); err != nil {
		t.Fatal(err)
	}
	if w.bytes != size || x.h.persistent.ram.size != 0 {
		t.Fatal("large response entered RAM", w.bytes)
	}
}

func TestS3TransportSigningPinAndStreaming(t *testing.T) {
	var stored []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("missing signed request")
		}
		if r.URL.Path != "/bucket/cache/key" {
			t.Error(r.URL.Path)
		}
		switch r.Method {
		case "PUT":
			stored, _ = io.ReadAll(r.Body)
			w.WriteHeader(200)
		case "GET":
			w.Header().Set("Content-Length", fmt.Sprint(len(stored)))
			w.Write(stored)
		case "DELETE":
			stored = nil
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	pin := sha256.Sum256(server.Certificate().RawSubjectPublicKeyInfo)
	c := S3Connection{Endpoint: server.URL, Bucket: "bucket", Prefix: "cache", Region: "auto", PathStyle: true, AccessKey: "test", SecretKey: "test", SPKI: hex.EncodeToString(pin[:])}
	s, err := newS3Objects(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "key", bytes.NewReader([]byte("body")), 4); err != nil {
		t.Fatal(err)
	}
	b, n, err := s.Open(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(b)
	b.Close()
	if n != 4 || string(got) != "body" {
		t.Fatal(n, string(got))
	}
	if err := s.Delete(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
	c.SPKI = strings.Repeat("0", 64)
	bad, _ := newS3Objects(c)
	if _, _, err := bad.Open(context.Background(), "key"); err == nil {
		t.Fatal("trusted wrong TLS pin")
	}
}

func TestPublicCDNReadFallbackAndClassification(t *testing.T) {
	x, private := persistentHarness(t, 0)
	public := &fileObjects{dir: t.TempDir()}
	p := x.h.persistent
	p.public = public
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Error("visitor credentials leaked")
		}
		w.Header().Set("Location", "https://other.invalid/")
		w.WriteHeader(302)
	}))
	defer server.Close()
	p.cdnURL = server.URL + "/"
	p.cdnClient, _ = objectHTTPClient("")
	x.do("GET", "/")
	expect(t, x.do("GET", "/"), "mf; hit; detail=s3")
	files, _ := os.ReadDir(public.dir)
	if len(files) != 1 {
		t.Fatal("public destination not used")
	}
	x.do("GET", "/cookie", "Cookie", "consent=yes")
	files, _ = os.ReadDir(private.dir)
	if len(files) != 1 {
		t.Fatal("cookie variant exposed")
	}
	records, _ := json.Marshal(p.config)
	if strings.Contains(string(records), "secret") {
		t.Fatal("credential exposed")
	}
}
