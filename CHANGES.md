# Changelog

## Unreleased

### Connection pooling

- Each repo now has its own HTTP connection pool (`Couchx.Pool`, backed by a
  dedicated Finch instance). Configure with `pool_size` (default 10),
  `pool_count` (default 1) and `connect_timeout` (default 30_000).
- Requests run in the calling process through Req. `Couchx.DbConnection` is no
  longer a GenServer, so large responses aren't copied between processes and a
  timed-out caller no longer leaves a busy worker behind.
- Couchx now starts an application (`Couchx.Application`) that runs an internal
  `Couchx.Pool.Registry`. The adapter's `ensure_all_started/2` starts it.

### Breaking changes

- Requires Req `~> 0.7` (was `~> 0.6`) and therefore Elixir `~> 1.15` (was
  `~> 1.12`). The pool and `:pool_timeout` are passed to Req as
  `finch: [name: ..., pool_timeout: ...]`, which Req 0.6 doesn't support and
  which avoids Req 0.7's deprecation warnings.
- `Couchx.DbConnection.start_link/1` and the GenServer callbacks are gone. Start
  a `Couchx.Pool` instead; every `Couchx.DbConnection` function takes the pool
  pid, name or `{:via, ...}` tuple (normally the repo `:pid`).
- The pool, not a connection process, is registered in `CouchxRegistry` under
  the repo `:name`.
- `:call_timeout` is now the HTTP receive timeout rather than a
  `GenServer.call/3` timeout. Waiting for a free connection has its own
  `:pool_timeout` (default 5_000). Both still exit with `{:timeout, _}`.
- `:timeout` no longer sets the TCP connect timeout per call (Req doesn't allow
  `connect_options` with a custom Finch pool). Use `connect_timeout` in the
  repo config.
- Requests are no longer retried automatically. Req used to retry failed GETs
  up to 3 times by default; pass `retry: :safe_transient` to restore that.

## 2.0.1

### Fixes (Req migration follow-ups)

- Stop forwarding CouchDB/adapter options (`:include_docs`, `:reduce`, `:module`, `:body`, …) into `Req.request!/1`, which rejects unknown options.
- Move view/query options from `raw_request/4` into the URL query string.
- Join base URL and path without introducing `//` (Req rejects those request targets; HTTPoison previously tolerated them when `database` was empty).
- Map legacy HTTPoison `:recv_timeout` to Req `:receive_timeout`.
- Map legacy `:timeout` to Req's connection timeout while retaining it as the
  Couchx GenServer call timeout.
- Increase the default Couchx call timeout so Req's transient retry backoff can
  complete, and support an explicit `:call_timeout`.
- Return the tuple required by Ecto from `ensure_all_started/2`.
- Avoid creating atoms from URL query-string keys.
- Merge duplicate query parameters with explicit options taking precedence.

### Tests

- Added Bypass-backed regression tests in `test/db_connection_test.exs` covering the cases above.

## 2.0.0

- Replace HTTPoison with Req (`req ~> 0.6`).
