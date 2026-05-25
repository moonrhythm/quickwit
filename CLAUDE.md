# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
go test ./...          # run all tests
go test -run TestName  # run a single test
go vet ./...           # static analysis
```

No build step is needed — this is a pure Go library with no external dependencies.

## Architecture

Single-file library (`quickwit.go`) that provides a Go client for the [Quickwit](https://quickwit.io) search engine. The `Client` struct exposes two operational modes:

### Ingest (async, buffered)

`Ingest(data...)` enqueues JSON-serializable items into an internal channel (`ingestBuffer`). Background workers (default 2) drain the channel and POST batches as NDJSON to `{endpoint}/ingest`. Flushing is triggered by batch size (default 1,000) or a timer (default 1 second), whichever comes first.

Key behaviors:
- **Backpressure**: blocks callers when the buffer is full unless `SetDiscard(true)` is set; discarded items trigger the `OnDiscard` callback.
- **413 handling**: when the server returns HTTP 413, the worker automatically reduces the effective batch size by 10% (down to a floor of 10% of the configured size) and re-flushes in smaller chunks while preserving record order. Batch size resets after `ResetBatchSizeAfter` (10 min) of no 413s.
- **Retries**: failed flushes retry with exponential backoff indefinitely during normal operation; `Close()` retries up to 5 times then discards.
- **Setup is lazy**: the background goroutines start on the first `Ingest` call via `sync.Once`.
- **`Close()`** is idempotent and safe to call before the first `Ingest`.

### Search (sync)

`Search(ctx, query, opts)` POSTs to `{endpoint}/search` and returns parsed hits, total count, and elapsed time. `SearchOpt` fields that are zero-valued/empty are omitted from the request body.

### Endpoint convention

The `endpoint` passed to `NewClient` should be `http://{host}/api/v1/{index_name}` — the client appends `/ingest` or `/search` directly.
