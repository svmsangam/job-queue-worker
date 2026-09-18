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

// Config holds internal parameters for the worker pool.
type Config struct {
	Concurrency  int
	QueueBuffer  int
	MaxRetries   int
	Processor    JobProcessor
	DLQPublisher DLQPublisher
	Logger       *slog.Logger
	Metrics      *metrics.Metrics
}

// Option is a function type that mutates Config.
type Option func(*Config)

func WithConcurrency(c int) Option {
	return func(cfg *Config) {
		if c > 0 {
			cfg.Concurrency = c
		}
	}
}

func WithQueueBuffer(b int) Option {
	return func(cfg *Config) {
		if b > 0 {
			cfg.QueueBuffer = b
		}
	}
}

func WithMaxRetries(r int) Option {
	return func(cfg *Config) {
		if r >= 0 {
			cfg.MaxRetries = r
		}
	}
}

func WithProcessor(p JobProcessor) Option {
	return func(cfg *Config) {
		cfg.Processor = p
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(cfg *Config) {
		cfg.Logger = l
	}
}

func WithDLQ(publisher DLQPublisher) Option {
	return func(cfg *Config) {
		cfg.DLQPublisher = publisher
	}
}

func WithMetrics(m *metrics.Metrics) Option {
	return func(cfg *Config) { cfg.Metrics = m }
}

// Pool represents the concurrent worker processing pool.
type Pool struct {
	cfg      Config
	jobChan  chan Job
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewPool initializes a worker pool using applied Functional Options.
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

// Start spawns worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.cfg.Logger.Info("worker pool started", slog.Int("concurrency", p.cfg.Concurrency))
}

// Submit enqueues a job into the buffered channel.
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

// Stop gracefully waits for in-flight tasks to finish.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wg.Wait()
	p.cfg.Logger.Info("worker pool stopped gracefully")
}

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
