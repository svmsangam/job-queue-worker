package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"job-queue/internal/worker"
)

func main() {
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// 1. Establish gRPC client connection pool
	grpcTarget := "localhost:50051"
	client, err := worker.NewProcessorClient(grpcTarget, logger)
	if err != nil {
		logger.Error("failed to create gRPC client", slog.Any("error", err))
		os.Exit(1)
	}
	defer client.Close()

	// 2. Instantiate strategy pattern processor
	processor := worker.NewGRPCJobProcessor(client)

	// 3. Initialize worker pool using Functional Options
	pool, err := worker.NewPool(
		worker.WithConcurrency(3),  // 3 parallel worker goroutines
		worker.WithQueueBuffer(10), // shock-absorber buffer channel size
		worker.WithProcessor(processor),
		worker.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to initialize worker pool", slog.Any("error", err))
		os.Exit(1)
	}

	// Context for managing pool lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start worker goroutines
	pool.Start(ctx)

	// 5. Submit sample batch of jobs concurrently
	go func() {
		for i := 1; i <= 6; i++ {
			job := worker.Job{
				ID:      fmt.Sprintf("job-%03d", i),
				Type:    "DATA_PROCESSING",
				Payload: []byte(fmt.Sprintf(`{"payload_id": %d}`, i)),
			}

			if err := pool.Submit(ctx, job); err != nil {
				logger.Error("failed to submit job", slog.String("job_id", job.ID), slog.Any("error", err))
			} else {
				logger.Info("job enqueued", slog.String("job_id", job.ID))
			}
		}
	}()

	// 6. Graceful shutdown handler
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan
	logger.Info("shutdown signal received, stopping worker pool...")

	// Drain remaining buffered jobs before exit
	pool.Stop()
	logger.Info("worker service stopped cleanly")
}
