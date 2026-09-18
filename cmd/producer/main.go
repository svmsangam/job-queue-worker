package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"job-queue/internal/producer"
	"job-queue/internal/worker"
	"job-queue/pkg/logger"
)

func main() {
	logHandler, err := logger.New(logger.Config{LokiURL: "http://localhost:3100/loki/api/v1/push", Service: "producer", Level: slog.LevelInfo})
	if err != nil {
		panic(err)
	}
	defer logHandler.Close(context.Background())
	logger := slog.New(logHandler)

	brokers := []string{"localhost:9092"}
	topic := "jobs.v1"

	p := producer.NewKafkaProducer(brokers, topic, logger)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Info("publishing test batch jobs to kafka...")

	for i := 1; i <= 5; i++ {
		jobID := fmt.Sprintf("job-kafka-%03d", i)
		jobPayload := worker.Job{
			ID:      jobID,
			Type:    "IMAGE_RESIZE",
			Payload: []byte(fmt.Sprintf(`{"image_url": "https://example.com/img_%d.jpg"}`, i)),
		}

		data, err := json.Marshal(jobPayload)
		if err != nil {
			logger.Error("failed to serialize job payload", slog.Any("error", err))
			continue
		}

		if err := p.PublishJob(ctx, jobID, data); err != nil {
			logger.Error("failed to publish job to kafka", slog.String("job_id", jobID), slog.Any("error", err))
		}
	}

	logger.Info("batch job publishing complete")
}
