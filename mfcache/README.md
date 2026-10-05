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
serving of any kind, no revalidation, no range or conditional handling (a hit is the whole stored
response), and no persistence. Request `Cache-Control` directives other than
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
