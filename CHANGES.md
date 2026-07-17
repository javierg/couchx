# Changelog

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
