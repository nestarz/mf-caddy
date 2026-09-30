# mf-caddy

Reproducible builds of [Caddy](https://github.com/caddyserver/caddy) with [`mfcache`](mfcache), the
shared HTTP cache of the `mf` edge, used on every `mf` ingress host.

Each release `caddy-<caddy>-mfcache-<version>` is built by `.github/workflows/caddy.yml` (run by
hand) from the commit it tags, for `linux/amd64` and `darwin/arm64`, with Go, xcaddy, and a fixed
`SOURCE_DATE_EPOCH` pinned in the workflow. The workflow runs the module's tests first and checks
that `http.handlers.mf_cache` is in the build. `SHA256SUMS` lists the archives and their `caddy`
entries; `mf` pins those digests.

## Why a module of our own

The existing Caddy caches were checked against the edge's contract (see [`mfcache`](mfcache)) and
none meets it by configuration: `caddyserver/cache-handler` (Souin) stores responses without
freshness under a default TTL, stores and replays `Set-Cookie`, stores `no-store` with `Expires` and
`private` with `Vary: Authorization`, and its in-memory storages are unbounded or bounded by count;
`sillygod/cdp-cache` replays `Set-Cookie`, stores other statuses, serves stale entries on 5xx and
sends no `Age`; the others are TTL-based or application-specific.

Releases up to `caddy-2.11.4-cache-0.17.0` carried cache-handler instead; `mf` no longer pins them.

Caddy is Apache-2.0 licensed; see its repository.
