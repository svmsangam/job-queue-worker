package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"job-queue/internal/worker"
)

func main() {
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

	// 2. Instantiate gRPC Job Processor Strategy
	processor := worker.NewGRPCJobProcessor(client)

	// 3. Ensure jobs.dlq exists before workers try to publish failed tasks
	if err := worker.EnsureTopicExists("localhost:9092", "jobs.dlq", 3, 1); err != nil {
		logger.Warn("dlq topic auto-creation skipped or topic exists", slog.Any("error", err))
	}

	// 4. Now initialize the DLQ Publisher safely
	dlqPublisher := worker.NewKafkaDLQPublisher([]string{"localhost:9092"}, "jobs.dlq", logger)
	defer dlqPublisher.Close()

	// 5. Initialize Worker Pool with Retries and Options
	pool, err := worker.NewPool(
		worker.WithConcurrency(3),
		worker.WithQueueBuffer(50),
		worker.WithMaxRetries(3),
		worker.WithProcessor(processor),
		worker.WithDLQ(dlqPublisher),
		worker.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to initialize worker pool", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start Worker Goroutines
	pool.Start(ctx)

	// 5. Start Kafka Consumer Loop
	brokers := []string{"localhost:9092"}
	consumer := worker.NewConsumer(brokers, "jobs.v1", "worker-group-1", pool, logger)
	defer consumer.Close()

	go func() {
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("consumer stopped unexpectedly", slog.Any("error", err))
		}
	}()

	// 6. Handle Graceful Shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan
	logger.Info("shutdown signal received, stopping services...")

	cancel()    // Stops Kafka consumer polling loop
	pool.Stop() // Drains in-flight worker channel jobs
	logger.Info("worker service shut down cleanly")
}
