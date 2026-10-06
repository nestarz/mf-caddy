# mfcache

`http.handlers.mf_cache`: the shared HTTP cache of the `mf` edge. It stores only what the origin
explicitly marks fresh and safe to share, in memory, bounded in bytes. It has no switch of its own:
the app's response headers decide.

```json
{"handler": "mf_cache", "max_bytes": 268435456}
```

`max_bytes` (default 256 MiB) bounds everything the store holds: bodies, headers and keys. Every
handler in the process with the same bound shares one store, which survives config reloads.

## Contract

1. **Explicit freshness only.** A response is stored only with `s-maxage`, `max-age` or `Expires`
   (RFC 9111 §4.2.1, in that order). There is no heuristic freshness and no default TTL. An invalid
   value, or one already reached by the response's age, stores nothing.
2. **Never stored:**
   - a response with `private`, `no-store` or `no-cache` (qualified or not);
   - a response with `Set-Cookie`;
   - a response to a request with `Authorization`, unless it has `public` or `s-maxage`;
   - a response to a request with `Cache-Control: no-store`;
   - a response to anything but `GET` (a `HEAD` is answered from a stored `GET`);
   - a status other than 200, 203, 204, 300, 301, 404, 405, 410, 414 or 501 (206 is not stored);
   - a response with `Vary: *`;
   - an entry above 1/16 of `max_bytes`, or a body that did not reach the client whole.
3. **Vary.** A stored response is served only to a request whose fields named by its `Vary`
   match those of the request that stored it (RFC 9111 §4.1), after list whitespace is removed.
4. **Key.** The host, the path and query, and the `X-Mf-Deployment` request header. The edge sets
   that header on every route and overwrites any client value, so a new deployment never serves an
   older one's entry. A successful unsafe request (`POST`, `PUT`, `DELETE`, …) drops the entries of
   its key (RFC 9111 §4.4).
5. **Bounded memory.** Entries live in memory only, in one least-recently-used list; storing past
   `max_bytes` evicts from its tail. Stale entries are dropped when looked up.
6. **Observability.** Every response gets an RFC 9211 `Cache-Status` member, after any member from
   upstream:
   - `mf; hit`;
   - `mf; fwd=uri-miss|vary-miss|stale; stored`;
   - `mf; fwd=uri-miss|vary-miss|stale; detail=<reason>`, the reason being one of `no-freshness`,
     `expired`, `private`, `no-store`, `no-cache`, `set-cookie`, `authorization`,
     `request-no-store`, `method`, `status`, `vary-star` or `too-large`;
   - `mf; fwd=method` for a method other than `GET` and `HEAD`.

   A hit carries `Age` as RFC 9111 §4.2.3 computes it, from the origin's `Date` and `Age` and the
   time the response took to arrive.

What it does not do: no request coalescing (concurrent misses each reach the origin), no stale
serving of any kind, no revalidation, or range or conditional handling (a hit is the whole stored
response). Request `Cache-Control` directives other than
`no-store` are ignored, as a shared cache in front of an origin may.

## Tests

```sh
go test ./...
```

Each contract point above has a test in `mfcache_test.go`.

## Tag purging

Set `scope` to a stable app identifier on each cache route. Set `purge_path` and
`purge_token_hash` (hex SHA-256) on a route for that scope. A `POST` with a bearer token and
`{"tags":["product:1"]}` removes entries whose `Cache-Tag` contains an exact matching tag.
The request permits 1–500 tags, at most 256 bytes each, in a body of at most 64 KiB.

A local purge returns 204 after all matching hosts, deployments and variants in that scope
are removed. It fences fills started before the purge. Other scopes remain cached.
The mf fleet API calls this endpoint on every ingress host and fails if any host does not
confirm. It stores only token hashes in Caddy configuration. No special response extension
or path rule makes private content cacheable.

## Persistent S3 bodies

Set `persistent` on each cache and purge handler to share a private local index and S3 bodies:

```json
{
  "handler": "mf_cache",
  "scope": "app",
  "persistent": {
    "connection_file": "/run/credentials/mf-proxy.service/cache-store.json",
    "index_path": "/var/lib/mf-cache/namespace/index.db",
    "ram_bytes": 33554432,
    "ram_entry_bytes": 131072,
    "stream_buffer_bytes": 65536,
    "max_object_bytes": 67108864,
    "max_store_bytes": 2147483648,
    "max_concurrent_reads": 64,
    "max_concurrent_fills": 4
  }
}
```

`ram_bytes: 0` disables RAM bodies. With persistence, `max_bytes` is unused. The legacy memory-only
mode remains available by omitting `persistent`. All handlers using an index share its budgets.

The protected connection file contains `private` and optionally `public` S3 connections, each
with `endpoint`, `bucket`, `prefix`, `region`, `path_style`, `access_key`, `secret_key`, optional
`session_token`, and optional `spki` (hex SHA-256 of a trusted TLS public key). `public_read_url`
optionally names an HTTPS CDN URL including the public object's prefix. Use a dedicated namespace
per host and never share an index between processes. Endpoint and bucket changes require a new index.
Budget or credential changes require a Caddy restart; route-only reloads keep the store.

Bodies stream from the S3 SDK or the CDN with bounded buffers. Only objects below `ram_entry_bytes`
and the RAM tier's per-entry bound may be copied into RAM. Cached HEAD responses need no body GET.
Hits carry `Cache-Status: mf; hit; detail=s3|cdn`; RAM hits retain `mf; hit`.

Fills stream to clients and a bounded private spool file, then upload from that seekable file.
The response is indexed only after the full body and PUT succeed. No unbounded upload goroutines
or queues are created. Fill saturation skips population; read saturation returns 503/Retry-After.
Each PUT has a two-minute deadline, reads five minutes, and backend response headers ten seconds.
Disconnects cancel S3 operations. An error after response bytes start aborts that response; it never
appends a new origin response to a partial cached body.

The embedded index persists freshness, hashed Vary values, and tag mappings, not response bodies.
Purge commits index removal before acknowledgement and fences in-flight fills and RAM promotions.
Object deletion is deferred; failed deletes retain their budget reservation. Entries expire, and
the oldest entries are evicted under storage pressure. Metadata is bounded to 8192 objects and 16 KiB
per response record. Temporary files are removed on completion or restart. A bounded S3 LIST sweep
also removes unknown objects older than 24 hours after index loss, under this host's prefix only.
Index loss causes misses; indexes are not shared between hosts. Changing storage location starts a
cold namespace; retire/clean the old location separately before removing its credentials.

Public CDN reads require a separate bucket. Only explicitly public responses to requests without
Cookie or Authorization and with no Vary other than Accept-Encoding enter it. General shared
variants stay private; private/no-store/Set-Cookie responses are never stored. Never expose the
artifact bucket. CDN reads receive no visitor headers or storage credentials and never follow
redirects. On a failed CDN open, MF tries the same immutable body through authenticated S3.
The browser keeps its original URL. Public immutable body URLs can remain in a CDN for their
transport TTL (one day); tag purging invalidates application URLs, not previously disclosed public
body URLs. Do not use this mode for revocable content.

R2's S3 API already has free egress. A custom domain adds CDN caching, not free VPS outbound traffic
or guaranteed sub-millisecond latency. Configure its cache rule for opaque object keys explicitly;
the module does not provision a CDN, publish a bucket, or require a Worker.

The private Caddy metrics registry exports `mf_cache_*_total` counters for RAM/S3/CDN hits,
streamed bytes, fills, skipped fills, errors, evictions, deleted objects, and purges.

Run `go test -race ./...`. Set `MF_TEST_RUSTFS` to a local RustFS executable to include a real
signed S3 fill/read/delete integration, in addition to the TLS and streaming fixtures.
