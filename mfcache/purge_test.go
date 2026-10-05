package mfcache

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestPurgeIsScopedAuthenticatedAndFencesFills(t *testing.T) {
	store := newStore(DefaultMaxBytes)
	handler := func(scope string) *Handler {
		return &Handler{Scope: scope, PurgePath: "/_mf/cache/" + scope + "/local", PurgeTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte(scope+"-secret"))), store: store, now: func() time.Time { return start }}
	}
	a, b := handler("a"), handler("b")
	calls := 0
	origin := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", "autokit, product:1")
		w.Header().Set("Vary", "Cookie")
		_, err := fmt.Fprintf(w, "%d", calls)
		return err
	})
	get := func(h *Handler, host, cookie string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "https://"+host+"/", nil)
		r.Header.Set("Cookie", cookie)
		w := httptest.NewRecorder()
		if err := h.ServeHTTP(w, r, origin); err != nil {
			t.Fatal(err)
		}
		return w
	}
	purge := func(token, body string) int {
		r := httptest.NewRequest("POST", "https://fleet"+a.PurgePath, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		if err := a.ServeHTTP(w, r, origin); err != nil {
			t.Fatal(err)
		}
		return w.Code
	}
	get(a, "custom.test", "")
	get(a, "custom.test", "consent=1")
	get(a, "revision.test", "")
	get(b, "other.test", "")
	epoch := store.generation()
	if got := purge("b-secret", `{"tags":["product:1"]}`); got != 404 {
		t.Fatal(got)
	}
	if got := purge("a-secret", `{"tags":["product:1"],"scope":"b"}`); got != 400 {
		t.Fatal(got)
	}
	expect(t, get(a, "custom.test", ""), "mf; hit")
	if got := purge("a-secret", `{"tags":["product:1"]}`); got != 204 {
		t.Fatal(got)
	}
	expect(t, get(b, "other.test", ""), "mf; hit")
	for index, sample := range [][2]string{{"custom.test", ""}, {"custom.test", "consent=1"}, {"revision.test", ""}} {
		want := "mf; fwd=uri-miss; stored"
		if index == 1 {
			want = "mf; fwd=vary-miss; stored"
		}
		expect(t, get(a, sample[0], sample[1]), want)
	}
	stale := &entry{key: "late", scope: "a", size: 1}
	store.putAt(stale, epoch)
	if len(store.byKey["late"]) != 0 {
		t.Fatal("stale fill survived purge")
	}
	if got := purge("a-secret", `{"tags":["unknown"]}`); got != 204 {
		t.Fatal(got)
	}
	expect(t, get(a, "custom.test", ""), "mf; hit")
}
