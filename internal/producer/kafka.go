package producer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewKafkaProducer initializes a thread-safe Kafka writer.
func NewKafkaProducer(brokers []string, topic string, logger *slog.Logger) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // Distribute messages across partitions based on load
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Synchronous writes to guarantee message delivery for jobs
	}

	return &KafkaProducer{
		writer: writer,
		logger: logger,
	}
}

// PublishJob serializes and publishes a job payload to Kafka.
func (p *KafkaProducer) PublishJob(ctx context.Context, jobID string, payload []byte) error {
	msg := kafka.Message{
		Key:   []byte(jobID), // Partition by JobID for ordering guarantees per job
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish message to kafka: %w", err)
	}

	p.logger.Info("published job to kafka",
		slog.String("job_id", jobID),
		slog.String("topic", p.writer.Topic),
	)

	return nil
}

// Close gracefully flushes buffered messages and closes the writer connection.
func (p *KafkaProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
