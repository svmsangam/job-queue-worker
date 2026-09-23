// Package logger provides a fan-out slog.Handler for console, daily local
// files, and optional asynchronous Loki delivery. It keeps remote logging
// off request and worker paths while retaining bounded shutdown semantics.
package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize = 256
	defaultBatchSize  = 50
	defaultFlushEvery = 200 * time.Millisecond
)

// Config controls the local file and Loki outputs, including the bounded Loki
// channel and batch flush policy.
type Config struct {
	LogDir        string
	LokiURL       string
	Service       string
	ChannelSize   int
	BatchSize     int
	FlushInterval time.Duration
	HTTPClient    *http.Client
	Level         slog.Level
}

// Handler fans each record out to all configured handlers. Child handlers are
// immutable, while atomic.Bool makes closed-state checks safe across callers
// and sync.Once makes Close idempotent during competing shutdown paths.
type Handler struct {
	handlers []slog.Handler
	closed   *atomic.Bool
	once     sync.Once
	closeErr error
}

// New creates a handler with console and daily local-file output plus optional
// asynchronous Loki output. Application log -> bounded LogChan -> batcher ->
// Loki HTTP request.
func New(cfg Config) (*Handler, error) {
	if cfg.LogDir == "" {
		cfg.LogDir = filepath.Join("pkg", "logger")
	}
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = defaultBufferSize
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushEvery
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}

	fileHandler, err := newFileHandler(cfg.LogDir, cfg.Level)
	if err != nil {
		return nil, err
	}
	consoleHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.Level,
	})

	return &Handler{
		handlers: []slog.Handler{fileHandler, consoleHandler, newLokiHandler(cfg)},
		closed:   &atomic.Bool{},
	}, nil
}

// Enabled reports whether at least one configured child accepts the level.
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle fans one record out to every enabled child handler and joins errors.
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	if h.closed != nil && h.closed.Load() {
		return errors.New("logger is closed")
	}
	var errs []error
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, record.Level) {
			if err := handler.Handle(ctx, record); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// WithAttrs returns a handler whose children include the supplied attributes.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	children := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		children[i] = handler.WithAttrs(attrs)
	}
	return &Handler{handlers: children, closed: h.closed}
}

// WithGroup returns a handler whose children write subsequent attributes in a
// named group.
func (h *Handler) WithGroup(name string) slog.Handler {
	children := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		children[i] = handler.WithGroup(name)
	}
	return &Handler{handlers: children, closed: h.closed}
}

// Close drains the Loki queue and closes the current local file. The caller's
// context bounds in-flight Loki requests so shutdown cannot block forever.
func (h *Handler) Close(ctx context.Context) error {
	h.once.Do(func() {
		h.closed.Store(true)
		for _, handler := range h.handlers {
			if closer, ok := handler.(interface{ Close(context.Context) error }); ok {
				if err := closer.Close(ctx); err != nil && h.closeErr == nil {
					h.closeErr = err
				}
			}
		}
	})
	return h.closeErr
}

type fileState struct {
	// mu serializes daily rotation and writes to the shared file handle.
	mu    sync.Mutex
	dir   string
	level slog.Level
	file  *os.File
	date  string
}

type fileHandler struct {
	state     *fileState
	configure func(slog.Handler) slog.Handler
}

// newFileHandler creates the shared state used by all derived file handlers.
func newFileHandler(dir string, level slog.Level) (*fileHandler, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	return &fileHandler{state: &fileState{dir: dir, level: level}}, nil
}

// Enabled reports whether the file handler accepts the requested level.
func (h *fileHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.state.level
}

// Handle rotates the daily file when needed and writes the structured record.
func (h *fileHandler) Handle(_ context.Context, record slog.Record) error {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()

	date := record.Time.Local().Format("02-01-2006")
	if record.Time.IsZero() {
		date = time.Now().Local().Format("02-01-2006")
	}
	if h.state.file == nil || h.state.date != date {
		if h.state.file != nil {
			_ = h.state.file.Close()
		}
		file, err := os.OpenFile(filepath.Join(h.state.dir, date+".txt"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		h.state.file, h.state.date = file, date
	}
	var base slog.Handler = slog.NewJSONHandler(h.state.file, &slog.HandlerOptions{Level: h.state.level})
	if h.configure != nil {
		base = h.configure(base)
	}
	return base.Handle(context.Background(), record)
}

func (h *fileHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.with(func(base slog.Handler) slog.Handler { return base.WithAttrs(attrs) })
}

func (h *fileHandler) WithGroup(name string) slog.Handler {
	return h.with(func(base slog.Handler) slog.Handler { return base.WithGroup(name) })
}

func (h *fileHandler) with(next func(slog.Handler) slog.Handler) *fileHandler {
	configure := func(base slog.Handler) slog.Handler {
		if h.configure != nil {
			base = h.configure(base)
		}
		return next(base)
	}
	return &fileHandler{state: h.state, configure: configure}
}

func (h *fileHandler) Close(context.Context) error {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	if h.state.file == nil {
		return nil
	}
	err := h.state.file.Close()
	h.state.file = nil
	return err
}

type lokiState struct {
	// LogChan decouples application logging from network latency; stop/done
	// coordinate drain completion, while stopOnce prevents double close.
	level      slog.Level
	url        string
	service    string
	client     *http.Client
	LogChan    chan lokiEntry
	batchSize  int
	flushEvery time.Duration
	stop       chan struct{}
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	stopOnce   sync.Once
	closeErr   atomic.Value
}

type lokiHandler struct {
	state     *lokiState
	configure func(slog.Handler) slog.Handler
}

type lokiEntry struct {
	timestamp string
	line      string
}

func newLokiHandler(cfg Config) *lokiHandler {
	workerContext, cancel := context.WithCancel(context.Background())
	state := &lokiState{
		level: cfg.Level, url: cfg.LokiURL, service: cfg.Service, client: cfg.HTTPClient,
		LogChan: make(chan lokiEntry, cfg.ChannelSize), batchSize: cfg.BatchSize,
		flushEvery: cfg.FlushInterval, stop: make(chan struct{}), done: make(chan struct{}),
		ctx: workerContext, cancel: cancel,
	}
	if state.url != "" {
		go state.run()
	}
	return &lokiHandler{state: state}
}

// Enabled reports whether Loki is configured and accepts the requested level.
func (h *lokiHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.state.url != "" && level >= h.state.level
}

// Handle serializes a record and performs a non-blocking enqueue to Loki.
func (h *lokiHandler) Handle(_ context.Context, record slog.Record) error {
	if h.state.url == "" {
		return nil
	}
	var buf bytes.Buffer
	var base slog.Handler = slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: h.state.level})
	if h.configure != nil {
		base = h.configure(base)
	}
	if err := base.Handle(context.Background(), record); err != nil {
		return err
	}
	entry := lokiEntry{timestamp: strconv.FormatInt(record.Time.UnixNano(), 10), line: buf.String()}
	select {
	case h.state.LogChan <- entry:
	default:
		// A full queue must never block application logging. Dropping a record
		// is preferable to deadlocking a request when Loki is unavailable.
	}
	return nil
}

func (h *lokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.with(func(base slog.Handler) slog.Handler { return base.WithAttrs(attrs) })
}

func (h *lokiHandler) WithGroup(name string) slog.Handler {
	return h.with(func(base slog.Handler) slog.Handler { return base.WithGroup(name) })
}

func (h *lokiHandler) with(next func(slog.Handler) slog.Handler) *lokiHandler {
	configure := func(base slog.Handler) slog.Handler {
		if h.configure != nil {
			base = h.configure(base)
		}
		return next(base)
	}
	return &lokiHandler{state: h.state, configure: configure}
}

func (h *lokiHandler) Close(ctx context.Context) error {
	if h.state.url == "" {
		return nil
	}
	h.state.stopOnce.Do(func() { close(h.state.stop) })
	select {
	case <-h.state.done:
	case <-ctx.Done():
		h.state.cancel()
		h.state.closeErr.Store(ctx.Err())
	}
	if err := h.state.closeErr.Load(); err != nil {
		return err.(error)
	}
	return nil
}

// run batches queued records and drains the channel before signaling done.
func (s *lokiState) run() {
	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()
	defer close(s.done)
	batch := make([]lokiEntry, 0, s.batchSize)
	flush := func() {
		if len(batch) > 0 {
			_ = s.push(batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case entry := <-s.LogChan:
			batch = append(batch, entry)
			if len(batch) >= s.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			for {
				select {
				case entry := <-s.LogChan:
					batch = append(batch, entry)
					if len(batch) >= s.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// push sends one batch to Loki using the state's cancellable HTTP context.
func (s *lokiState) push(entries []lokiEntry) error {
	values := make([][]string, 0, len(entries))
	for _, entry := range entries {
		values = append(values, []string{entry.timestamp, entry.line})
	}
	payload := struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"streams"`
	}{Streams: []struct {
		Stream map[string]string `json:"stream"`
		Values [][]string        `json:"values"`
	}{{Stream: map[string]string{"service": s.service}, Values: values}}}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("loki returned %s", response.Status)
	}
	return nil
}
