# mf-caddy

Reproducible builds of [Caddy](https://github.com/caddyserver/caddy) with the
[cache-handler](https://github.com/caddyserver/cache-handler) module, used as the edge of `mf` hosts.

Each release `caddy-<caddy>-cache-<cache-handler>` is built by `.github/workflows/caddy.yml` (run by
hand) for `linux/amd64` and `darwin/arm64`, with Go, xcaddy, and a fixed `SOURCE_DATE_EPOCH` pinned in
the workflow. `SHA256SUMS` lists the archives and their `caddy` entries; `mf` pins those digests.

Caddy and cache-handler are Apache-2.0 licensed; see their repositories.
