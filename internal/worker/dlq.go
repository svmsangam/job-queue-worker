package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// DLQMessage represents the structured payload sent to the dead-letter topic.
type DLQMessage struct {
	OriginalJob Job       `json:"original_job"`
	ErrorReason string    `json:"error_reason"`
	FailedAt    time.Time `json:"failed_at"`
	Attempts    int       `json:"attempts"`
}

// DLQPublisher defines the interface for forwarding unprocessable jobs.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, job Job, reason error, attempts int) error
	Close() error
}

type KafkaDLQPublisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func EnsureTopicExists(brokerHostPort string, topic string, numPartitions int, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokerHostPort)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to fetch controller broker: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	return nil
}

// NewKafkaDLQPublisher initializes a Kafka writer directed at the DLQ topic.
func NewKafkaDLQPublisher(brokers []string, dlqTopic string, logger *slog.Logger) *KafkaDLQPublisher {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Synchronous writes to guarantee DLQ delivery before offset commit
	}

	return &KafkaDLQPublisher{
		writer: writer,
		logger: logger,
	}
}

func (p *KafkaDLQPublisher) PublishDLQ(ctx context.Context, job Job, reason error, attempts int) error {
	dlqPayload := DLQMessage{
		OriginalJob: job,
		ErrorReason: reason.Error(),
		FailedAt:    time.Now().UTC(),
		Attempts:    attempts,
	}

	bytes, err := json.Marshal(dlqPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(job.ID),
		Value: bytes,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to DLQ topic %s: %w", p.writer.Topic, err)
	}

	p.logger.Warn("job routed to dead letter queue",
		slog.String("job_id", job.ID),
		slog.String("dlq_topic", p.writer.Topic),
		slog.String("reason", reason.Error()),
	)

	return nil
}

func (p *KafkaDLQPublisher) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
