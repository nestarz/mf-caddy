package mfcache

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxDelta caps a delta-seconds value, as RFC 9111 §1.2.2 allows.
const maxDelta = 1<<31 - 1

// decision is what a response says about storing it, taken when its headers are written.
type decision struct {
	// detail says why the response is not stored; it is empty when it is.
	detail     string
	status     int
	header     http.Header
	vary       []string
	lifetime   time.Duration
	initialAge time.Duration
	responded  time.Time
}

// decide applies the contract to a response for r: explicit freshness, no private, no-store,
// no-cache or Set-Cookie, a public or s-maxage answer to Authorization, a status RFC 9110 calls
// heuristically cacheable (206 aside), and no `Vary: *`.
func decide(r *http.Request, status int, header http.Header, requested, responded time.Time, limit int) decision {
	d := decision{status: status, responded: responded}
	cc := directives(header.Values("Cache-Control"))
	d.vary = varyNames(header)
	switch {
	case r.Method != http.MethodGet:
		d.detail = "method"
	case has(directives(r.Header.Values("Cache-Control")), "no-store"):
		d.detail = "request-no-store"
	case !cacheableStatus(status):
		d.detail = "status"
	case has(cc, "no-store"):
		d.detail = "no-store"
	case has(cc, "private"):
		d.detail = "private"
	case has(cc, "no-cache"):
		d.detail = "no-cache"
	case len(header.Values("Set-Cookie")) > 0:
		d.detail = "set-cookie"
	case r.Header.Get("Authorization") != "" && !has(cc, "public") && !has(cc, "s-maxage"):
		d.detail = "authorization"
	case slices.Contains(d.vary, "*"):
		d.detail = "vary-star"
	}
	if d.detail != "" {
		return d
	}
	lifetime, explicit := freshnessLifetime(cc, header, responded)
	if !explicit {
		d.detail = "no-freshness"
		return d
	}
	if n, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64); err == nil && n > int64(limit) {
		d.detail = "too-large"
		return d
	}
	d.header = storedHeader(header, responded)
	d.lifetime = lifetime
	d.initialAge = initialAge(d.header, requested, responded)
	if d.lifetime <= d.initialAge {
		d.detail = "expired"
	}
	return d
}

// entry is the stored form of the decided response with the body the client received.
func (d decision) entry(key string, request http.Header, body []byte) *entry {
	e := &entry{
		key:        key,
		status:     d.status,
		header:     d.header,
		body:       body,
		vary:       make(map[string]string, len(d.vary)),
		lifetime:   d.lifetime,
		initialAge: d.initialAge,
		responded:  d.responded,
	}
	for _, name := range d.vary {
		e.vary[name] = fieldValue(request, name)
	}
	e.size = e.bytes()
	return e
}

// cacheableStatus lists the RFC 9110 §15.1 heuristically cacheable codes, less 206, which would
// need range handling.
func cacheableStatus(status int) bool {
	switch status {
	case 200, 203, 204, 300, 301, 404, 405, 410, 414, 501:
		return true
	}
	return false
}

// freshnessLifetime follows RFC 9111 §4.2.1 for a shared cache: s-maxage, then max-age, then
// Expires less Date. It reports false when the response has none of them; an invalid value counts
// as already stale.
func freshnessLifetime(cc map[string]string, header http.Header, responded time.Time) (time.Duration, bool) {
	if v, ok := cc["s-maxage"]; ok {
		return deltaSeconds(v), true
	}
	if v, ok := cc["max-age"]; ok {
		return deltaSeconds(v), true
	}
	expires := header.Values("Expires")
	if len(expires) == 0 {
		return 0, false
	}
	t, err := http.ParseTime(expires[0])
	if err != nil {
		return 0, true
	}
	return t.Sub(dateValue(header, responded)), true
}

// initialAge is the corrected_initial_age of RFC 9111 §4.2.3.
func initialAge(header http.Header, requested, responded time.Time) time.Duration {
	apparent := max(0, responded.Sub(dateValue(header, responded)))
	var ageValue time.Duration
	if n, err := strconv.ParseInt(strings.TrimSpace(header.Get("Age")), 10, 64); err == nil && n >= 0 {
		ageValue = time.Duration(min(n, maxDelta)) * time.Second
	}
	return max(apparent, ageValue+responded.Sub(requested))
}

// dateValue is the response's Date, or the time it arrived when it has none.
func dateValue(header http.Header, responded time.Time) time.Time {
	if t, err := http.ParseTime(header.Get("Date")); err == nil {
		return t
	}
	return responded
}

// deltaSeconds parses a non-negative delta-seconds; anything else is a zero lifetime.
func deltaSeconds(v string) time.Duration {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return time.Duration(min(n, maxDelta)) * time.Second
}

// directives parses Cache-Control field lines into lower-case names and unquoted values; the first
// occurrence of a directive wins.
func directives(lines []string) map[string]string {
	cc := map[string]string{}
	for _, line := range lines {
		for _, part := range splitList(line) {
			name, value, _ := strings.Cut(part, "=")
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			if _, seen := cc[name]; !seen {
				cc[name] = strings.Trim(strings.TrimSpace(value), `"`)
			}
		}
	}
	return cc
}

// has reports a directive, with or without a value.
func has(cc map[string]string, name string) bool {
	_, ok := cc[name]
	return ok
}

// splitList splits a comma-separated field value, leaving commas inside quoted strings alone.
func splitList(v string) []string {
	var parts []string
	quoted, start := false, 0
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '"':
			quoted = !quoted
		case '\\':
			if quoted {
				i++
			}
		case ',':
			if !quoted {
				parts = append(parts, v[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, v[start:])
}

// varyNames lists the canonical, sorted, distinct field names of the response's Vary.
func varyNames(header http.Header) []string {
	var names []string
	for _, line := range header.Values("Vary") {
		for _, name := range strings.Split(line, ",") {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, http.CanonicalHeaderKey(name))
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// fieldValue is a request field's combined value with list whitespace removed; an absent field
// differs from an empty one.
func fieldValue(header http.Header, name string) string {
	values := header.Values(name)
	if len(values) == 0 {
		return "\x00"
	}
	var parts []string
	for _, v := range values {
		for _, p := range strings.Split(v, ",") {
			parts = append(parts, strings.TrimSpace(p))
		}
	}
	return strings.Join(parts, ",")
}

// hopByHop fields describe one connection and are never stored.
var hopByHop = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// storedHeader copies the origin's header without hop-by-hop fields, and with a Date.
func storedHeader(header http.Header, responded time.Time) http.Header {
	stored := header.Clone()
	for _, line := range header.Values("Connection") {
		for _, name := range strings.Split(line, ",") {
			stored.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopByHop {
		stored.Del(name)
	}
	if stored.Get("Date") == "" {
		stored.Set("Date", responded.UTC().Format(http.TimeFormat))
	}
	return stored
}
