package quickwit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	IngestBufferSize       = 10000
	IngestBatchSize        = 1000
	IngestMaxDelay         = time.Second
	IngestConcurrent       = 2
	IngestTimeout          = 15 * time.Second
	ReduceBatchSizeToRatio = 0.9 // reduce 10% of the batch size
	ReduceBatchSizeMin     = 0.1 // do not reduce below 10% of the default batch size
	ResetBatchSizeAfter    = 10 * time.Minute
)

type OnDiscardFunc func(any)

type Client struct {
	client              *http.Client
	auth                func(req *http.Request)
	endpoint            string // http://{host}/api/v1/{index_name}
	batchSize           int
	maxDelay            time.Duration
	ingestBufferSize    int
	ingestTimeout       time.Duration
	discard             bool
	concurrent          int
	ingestBuffer        chan any
	defaultClient       *http.Client
	onceDefaultClient   sync.Once
	onceSetup           sync.Once
	onceClose           sync.Once
	stopWg              sync.WaitGroup
	closeSignal         chan struct{}
	onDiscard           OnDiscardFunc
	autoReduceBatchSize bool
}

func NewClient(endpoint string) *Client {
	return &Client{
		endpoint:    endpoint,
		closeSignal: make(chan struct{}),
	}
}

func (c *Client) SetAuth(auth func(req *http.Request)) {
	c.auth = auth
}

func (c *Client) SetHTTPClient(client *http.Client) {
	c.client = client
}

func (c *Client) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

func (c *Client) SetMaxDelay(maxDelay time.Duration) {
	c.maxDelay = maxDelay
}

func (c *Client) SetIngestBufferSize(size int) {
	c.ingestBufferSize = size
}

func (c *Client) SetIngestTimeout(timeout time.Duration) {
	c.ingestTimeout = timeout
}

func (c *Client) SetDiscard(discard bool) {
	c.discard = discard
}

func (c *Client) SetConcurrent(concurrent int) {
	c.concurrent = concurrent
}

func (c *Client) SetAutoReduceBatchSize(autoReduceBatchSize bool) {
	c.autoReduceBatchSize = autoReduceBatchSize
}

func (c *Client) OnDiscard(f OnDiscardFunc) {
	c.onDiscard = f
}

func (c *Client) httpClient() *http.Client {
	if c.client != nil {
		return c.client
	}
	// When no client is supplied, fall back to a tuned default instead of
	// http.DefaultClient. Its transport's MaxIdleConnsPerHost defaults to 2,
	// which would close (and force a fresh handshake on) every connection
	// beyond the first two when SetConcurrent raises the worker count. Sizing
	// the idle pool to the worker count lets each worker keep its connection
	// hot for reuse. Built once, lazily, so the pool is shared across workers.
	c.onceDefaultClient.Do(func() {
		c.defaultClient = c.newDefaultClient()
	})
	return c.defaultClient
}

func (c *Client) newDefaultClient() *http.Client {
	concurrent := c.getConcurrent()

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = concurrent
	if t.MaxIdleConns < concurrent {
		t.MaxIdleConns = concurrent
	}

	// No client-level timeout: the per-request context deadline from
	// getIngestTimeout already bounds each flush, matching prior behavior.
	return &http.Client{Transport: t}
}

func (c *Client) getMaxDelay() time.Duration {
	if c.maxDelay <= 0 {
		return IngestMaxDelay
	}
	return c.maxDelay
}

func (c *Client) getBatchSize() int {
	if c.batchSize <= 0 {
		return IngestBatchSize
	}
	return c.batchSize
}

func (c *Client) getConcurrent() int {
	if c.concurrent <= 0 {
		return IngestConcurrent
	}
	return c.concurrent
}

func (c *Client) getIngestTimeout() time.Duration {
	if c.ingestTimeout <= 0 {
		return IngestTimeout
	}
	return c.ingestTimeout
}

func (c *Client) doAuth(req *http.Request) {
	if c.auth != nil {
		c.auth(req)
	}
}

func (c *Client) invokeOnDiscard(data any) {
	if c.onDiscard != nil {
		c.onDiscard(data)
	}
}

// Ingest sends data to the quickwit server.
// The data can be any type, and will be marshalled to JSON.
// The data will be buffered until the buffer is full, then sent to the server.
// If the buffer is full, Ingest will block until the buffer is no longer full.
func (c *Client) Ingest(data ...any) {
	c.onceSetup.Do(c.setup)
	for _, x := range data {
		if c.discard {
			select {
			case c.ingestBuffer <- x:
			default:
				c.invokeOnDiscard(x)
			}
		} else {
			c.ingestBuffer <- x
		}
	}
}

func (c *Client) Close() {
	c.onceSetup.Do(c.setup)
	c.onceClose.Do(func() {
		close(c.closeSignal)
		close(c.ingestBuffer)
	})
	c.stopWg.Wait()
}

func (c *Client) setup() {
	if c.ingestBufferSize <= 0 {
		c.ingestBufferSize = IngestBufferSize
	}
	c.ingestBuffer = make(chan any, c.ingestBufferSize)

	concurrent := c.getConcurrent()
	c.stopWg.Add(concurrent)
	for range concurrent {
		go c.loop()
	}
}

func (c *Client) loop() {
	defer c.stopWg.Done()

	var buf bytes.Buffer
	jsonEnc := json.NewEncoder(&buf)

	batchSize := c.getBatchSize()
	buffer := make([]any, 0, batchSize)
	var resetBatchSizeAfter time.Time

	endpoint := c.endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	endpoint = endpoint + "/ingest"

	flush := func(buffer []any) bool {
		if len(buffer) == 0 {
			return true
		}

		buf.Reset()

		var encodeFailures []any
		for _, x := range buffer {
			if err := jsonEnc.Encode(x); err != nil {
				slog.Error("quickwit: failed to encode record, discarding", "error", err)
				encodeFailures = append(encodeFailures, x)
				continue
			}
		}

		// All items were unencodable — discard them and report success so the
		// caller clears the buffer and does not retry with the same items.
		if buf.Len() == 0 {
			for _, x := range encodeFailures {
				c.invokeOnDiscard(x)
			}
			return true
		}

		ctx := context.Background()
		cancel := context.CancelFunc(func() {})
		if t := c.getIngestTimeout(); t != 0 {
			ctx, cancel = context.WithTimeout(ctx, t)
		}
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf.Bytes()))
		if err != nil {
			return false
		}
		c.doAuth(req)

		resp, err := c.httpClient().Do(req)
		if err != nil {
			return false
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Error("quickwit: ingest status not ok", "status", resp.Status)

			if resp.StatusCode == http.StatusRequestEntityTooLarge {
				if c.autoReduceBatchSize {
					beforeSize := batchSize
					batchSize = int(float64(batchSize) * ReduceBatchSizeToRatio)
					defaultSize := c.getBatchSize()
					minimumSize := int(float64(defaultSize) * ReduceBatchSizeMin)
					if batchSize < minimumSize {
						batchSize = minimumSize
					}
					resetBatchSizeAfter = time.Now().Add(ResetBatchSizeAfter)
					slog.Info("quickwit: auto reduce batch size",
						"new", batchSize,
						"old", beforeSize,
						"default", defaultSize,
						"minimum", minimumSize,
						"resetAfter", resetBatchSizeAfter.Format(time.RFC3339),
					)
				}
			}

			return false
		}

		// HTTP succeeded — now safe to discard items that failed encoding.
		for _, x := range encodeFailures {
			c.invokeOnDiscard(x)
		}

		if !resetBatchSizeAfter.IsZero() && time.Now().After(resetBatchSizeAfter) {
			beforeSize := batchSize
			batchSize = c.getBatchSize()
			resetBatchSizeAfter = time.Time{}
			slog.Info("quickwit: reset batch size", "batchSize", batchSize, "old", beforeSize)
		}

		return true
	}

	// flushOversize when buffer is oversize (from auto batch resize)
	// split buffer into smaller parts then flush each part separately
	flushOversize := func() bool {
		if len(buffer) == 0 {
			return true
		}

		if len(buffer) <= batchSize {
			if !flush(buffer) {
				return false
			}
			buffer = buffer[:0]
			return true
		}

		var processed int

		for chunk := range slices.Chunk(buffer, batchSize) {
			if !flush(chunk) {
				break
			}
			processed += len(chunk)
		}

		// drop the flushed prefix, keeping unprocessed records in their
		// original order at the front of the buffer, and clear the now
		// unused tail so flushed records can be garbage collected
		oldLen := len(buffer)
		remaining := copy(buffer, buffer[processed:])
		clear(buffer[remaining:oldLen])
		buffer = buffer[:remaining]
		return len(buffer) == 0
	}

	// retryFlush attempts to flush the buffer with retries
	// isClosing indicates if this is a final flush during shutdown
	retryFlush := func(isClosing bool) bool {
		// If not closing, retry indefinitely until success
		if !isClosing {
			attempt := 0
			backoff := 100 * time.Millisecond

			for {
				// close signal during retry
				select {
				default:
				case <-c.closeSignal:
					goto closing
				}

				if flushOversize() {
					return true
				}

				attempt++
				slog.Info("quickwit: flush failed, retrying indefinitely", "attempt", attempt)
				time.Sleep(backoff)
				// Exponential backoff with a cap
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > time.Second {
					backoff = time.Second
				}
			}
		}

	closing:

		// For closing case, use limited retries
		maxRetries := 5
		backoff := 100 * time.Millisecond

		for i := 0; i < maxRetries; i++ {
			if flushOversize() {
				return true
			}

			// Don't sleep on the last attempt
			if i < maxRetries-1 {
				slog.Info("quickwit: flush failed while closing, retrying", "attempt", i+1, "maxRetries", maxRetries)
				time.Sleep(backoff)
				// Exponential backoff with a cap
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > time.Second {
					backoff = time.Second
				}
			}
		}

		return false
	}

	ticker := time.NewTicker(c.getMaxDelay())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			flushOversize()
		case x, ok := <-c.ingestBuffer:
			if !ok { // channel closed
				if !retryFlush(true) {
					slog.Error("quickwit: flush failed while closing")
					for _, x := range buffer {
						c.invokeOnDiscard(x)
					}
				}
				return
			}
			buffer = append(buffer, x)
			if len(buffer) >= batchSize {
				retryFlush(false)
			}
		}
	}
}

type SearchOpt struct {
	StartTimestamp int64
	EndTimestamp   int64
	StartOffset    int64
	MaxHits        int64
	SearchField    []string
	SnippetFields  []string
	SortBy         string
	Format         string
}

type SearchResult struct {
	Hits              json.RawMessage
	NumHits           int64
	ElapsedTimeMicros int64
}

type searchRequestQueryString struct {
	Query          string   `json:"query"`
	StartTimestamp *int64   `json:"start_timestamp,omitempty"`
	EndTimestamp   *int64   `json:"end_timestamp,omitempty"`
	StartOffset    *int64   `json:"start_offset,omitempty"`
	MaxHits        *int64   `json:"max_hits,omitempty"`
	SearchField    []string `json:"search_field,omitempty"`
	SnippetFields  []string `json:"snippet_fields,omitempty"`
	SortBy         string   `json:"sort_by,omitempty"`
	Format         *string  `json:"format,omitempty"`
}

type searchResponseRest struct {
	Hits              json.RawMessage `json:"hits"`
	NumHits           int64           `json:"num_hits"`
	ElapsedTimeMicros int64           `json:"elapsed_time_micros"`
}

func (c *Client) Search(ctx context.Context, query string, opt *SearchOpt) (*SearchResult, error) {
	params := searchRequestQueryString{
		Query:  query,
		Format: Ptr("json"),
	}
	if opt != nil {
		if opt.StartTimestamp != 0 {
			params.StartTimestamp = &opt.StartTimestamp
		}
		if opt.EndTimestamp != 0 {
			params.EndTimestamp = &opt.EndTimestamp
		}
		if opt.StartOffset != 0 {
			params.StartOffset = &opt.StartOffset
		}
		if opt.MaxHits != 0 {
			params.MaxHits = &opt.MaxHits
		}
		params.SearchField = opt.SearchField
		params.SnippetFields = opt.SnippetFields
		params.SortBy = opt.SortBy
		if opt.Format != "" {
			params.Format = &opt.Format
		}
	}

	reqBody, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.endpoint, "/")+"/search", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.doAuth(req)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("quickwit: search status not ok, code: %d", resp.StatusCode)
	}

	var res searchResponseRest
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &SearchResult{
		Hits:              res.Hits,
		NumHits:           res.NumHits,
		ElapsedTimeMicros: res.ElapsedTimeMicros,
	}, nil
}

func Ptr[T any](t T) *T {
	return &t
}
