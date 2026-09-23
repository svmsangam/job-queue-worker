// Package main assembles the worker service: gRPC client, retrying worker pool,
// Kafka consumer, DLQ publisher, and Prometheus metrics endpoint.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"job-queue/internal/worker"
	"job-queue/pkg/logger"
	"job-queue/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// main starts service dependencies in data-flow order and coordinates graceful
// shutdown. Kafka -> consumer -> worker pool -> gRPC processor/DLQ.
func main() {
	logHandler, err := logger.New(logger.Config{LokiURL: "http://localhost:3100/loki/api/v1/push", Service: "worker", Level: slog.LevelInfo})
	if err != nil {
		panic(err)
	}
	defer logHandler.Close(context.Background())
	logger := slog.New(logHandler)
	serviceMetrics := metrics.New()
	metricsServer := &http.Server{Addr: ":2112", Handler: promhttp.HandlerFor(serviceMetrics.Registry, promhttp.HandlerOpts{})}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped unexpectedly", slog.Any("error", err))
		}
	}()

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
		worker.WithMetrics(serviceMetrics),
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down metrics server", slog.Any("error", err))
	}
	logger.Info("worker service shut down cleanly")
}
