// Package worker owns the asynchronous job execution pipeline. It decouples
// Kafka polling from processing with a bounded channel, runs a fixed number of
// goroutines, retries transient failures, and routes permanent failures to a
// dead-letter publisher before acknowledging the source message.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"job-queue/pkg/metrics"
)

// Config holds the worker pool's concurrency, buffering, retry, processing,
// dead-letter, logging, and metrics dependencies.
type Config struct {
	Concurrency  int
	QueueBuffer  int
	MaxRetries   int
	Processor    JobProcessor
	DLQPublisher DLQPublisher
	Logger       *slog.Logger
	Metrics      *metrics.Metrics
}

// Option mutates a Pool configuration during construction.
type Option func(*Config)

// WithConcurrency sets the number of jobs processed in parallel.
func WithConcurrency(c int) Option {
	return func(cfg *Config) {
		if c > 0 {
			cfg.Concurrency = c
		}
	}
}

// WithQueueBuffer sets the capacity of the channel between Kafka and workers.
// The buffer absorbs short bursts while still applying backpressure when full.
func WithQueueBuffer(b int) Option {
	return func(cfg *Config) {
		if b > 0 {
			cfg.QueueBuffer = b
		}
	}
}

// WithMaxRetries sets the number of processor attempts for each job.
func WithMaxRetries(r int) Option {
	return func(cfg *Config) {
		if r >= 0 {
			cfg.MaxRetries = r
		}
	}
}

// WithProcessor supplies the strategy that performs the actual job work.
func WithProcessor(p JobProcessor) Option {
	return func(cfg *Config) {
		cfg.Processor = p
	}
}

// WithLogger supplies the structured logger used by the pool.
func WithLogger(l *slog.Logger) Option {
	return func(cfg *Config) {
		cfg.Logger = l
	}
}

// WithDLQ supplies the publisher used after all processing attempts fail.
func WithDLQ(publisher DLQPublisher) Option {
	return func(cfg *Config) {
		cfg.DLQPublisher = publisher
	}
}

// WithMetrics supplies the Prometheus collectors updated by the pool.
func WithMetrics(m *metrics.Metrics) Option {
	return func(cfg *Config) { cfg.Metrics = m }
}

// Pool represents the concurrent worker processing pool. jobChan provides
// bounded handoff and backpressure; stopCh broadcasts shutdown; sync.Once
// makes Stop idempotent; and the WaitGroup ensures every worker has drained
// before shutdown returns.
type Pool struct {
	cfg      Config
	jobChan  chan Job
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewPool initializes a worker pool using functional options and rejects a
// missing processor because workers cannot make progress without a strategy.
func NewPool(opts ...Option) (*Pool, error) {
	cfg := Config{
		Concurrency: 5,
		QueueBuffer: 100,
		MaxRetries:  3,
		Logger:      slog.Default(),
		Metrics:     metrics.New(),
	}

	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}

	if cfg.Processor == nil {
		return nil, errors.New("worker pool requires a non-nil JobProcessor strategy")
	}

	return &Pool{
		cfg:     cfg,
		jobChan: make(chan Job, cfg.QueueBuffer),
		stopCh:  make(chan struct{}),
	}, nil
}

// Start spawns the configured workers. Kafka consumer -> buffered jobChan ->
// worker goroutine -> processor/DLQ -> optional Kafka offset acknowledgment.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.cfg.Logger.Info("worker pool started", slog.Int("concurrency", p.cfg.Concurrency))
}

// Submit enqueues a job or returns when the caller's context is canceled or
// the pool has stopped. The select prevents a blocked producer from surviving
// shutdown indefinitely.
func (p *Pool) Submit(ctx context.Context, job Job) error {
	p.cfg.Metrics.QueueDepth.Inc()
	select {
	case <-ctx.Done():
		p.cfg.Metrics.QueueDepth.Dec()
		return ctx.Err()
	case <-p.stopCh:
		p.cfg.Metrics.QueueDepth.Dec()
		return errors.New("worker pool is stopped")
	case p.jobChan <- job:
		return nil
	}
}

// Stop closes the shutdown broadcast once and waits for workers to drain jobs
// already accepted into the channel. It does not interrupt a running job.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wg.Wait()
	p.cfg.Logger.Info("worker pool stopped gracefully")
}

// worker consumes jobs until shutdown, then drains the buffered channel before
// returning so accepted work is not abandoned during a graceful stop.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case job := <-p.jobChan:
			p.processJob(ctx, job, id)
		case <-p.stopCh:
			for {
				select {
				case job := <-p.jobChan:
					p.processJob(ctx, job, id)
				default:
					return
				}
			}
		}
	}
}

// processJob executes one job, updates concurrent metrics, publishes permanent
// failures to the DLQ, and acknowledges Kafka only after successful processing.
func (p *Pool) processJob(ctx context.Context, job Job, id int) {
	p.cfg.Metrics.QueueDepth.Dec()
	p.cfg.Metrics.ActiveWorkers.Inc()
	started := time.Now()
	defer func() {
		p.cfg.Metrics.ActiveWorkers.Dec()
		p.cfg.Metrics.JobDuration.Add(time.Since(started).Seconds())
	}()

	p.cfg.Logger.Info("processing job", slog.Int("worker_id", id), slog.String("job_id", job.ID))

	err := p.executeWithRetry(ctx, job, id)
	if err != nil {
		p.cfg.Metrics.JobFailed.Inc()
		p.cfg.Logger.Error("job failed permanently after retries",
			slog.Int("worker_id", id),
			slog.String("job_id", job.ID),
			slog.Any("error", err),
		)
		// Route failed job to DLQ if configured
		if p.cfg.DLQPublisher != nil {
			if dlqErr := p.cfg.DLQPublisher.PublishDLQ(ctx, job, err, p.cfg.MaxRetries); dlqErr != nil {
				p.cfg.Logger.Error("failed to publish job to DLQ",
					slog.String("job_id", job.ID),
					slog.Any("error", dlqErr),
				)
				// Skip Ack if DLQ fails so message will be retried on consumer restart
				return
			}
		}
		// Non-committed offsets will cause Kafka to redeliver this message to another consumer on restart
		return
	}

	// Execute acknowledgment callback (commits offset in Kafka) ONLY after successful execution
	if job.Ack != nil {
		if ackErr := job.Ack(ctx); ackErr != nil {
			p.cfg.Logger.Error("failed to acknowledge job completion",
				slog.String("job_id", job.ID),
				slog.Any("error", ackErr),
			)
			p.cfg.Metrics.JobFailed.Inc()
			return
		}
	}

	p.cfg.Metrics.JobProcessed.Inc()
	p.cfg.Logger.Info("job completed successfully", slog.Int("worker_id", id), slog.String("job_id", job.ID))
}

// executeWithRetry executes the strategy with exponential backoff retries.
// Context cancellation interrupts the backoff so shutdown remains responsive.
func (p *Pool) executeWithRetry(ctx context.Context, job Job, workerID int) error {
	var err error
	backoff := 200 * time.Millisecond

	for attempt := 1; attempt <= p.cfg.MaxRetries; attempt++ {
		err = p.cfg.Processor.Process(ctx, job)
		if err == nil {
			return nil // Success
		}

		p.cfg.Logger.Warn("job processing failed, backing off and retrying",
			slog.Int("worker_id", workerID),
			slog.String("job_id", job.ID),
			slog.Int("attempt", attempt),
			slog.Int("max_retries", p.cfg.MaxRetries),
			slog.Any("error", err),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2 // Exponential backoff (200ms, 400ms, 800ms...)
		}
	}

	return fmt.Errorf("exceeded max retries (%d): %w", p.cfg.MaxRetries, err)
}
