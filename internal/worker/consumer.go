package worker

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	pool   *Pool
	logger *slog.Logger
}

func NewConsumer(brokers []string, topic string, groupID string, pool *Pool, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10,
		MaxBytes:       10e6,
		CommitInterval: 0, // Disable automatic background commits for manual control
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader: reader,
		pool:   pool,
		logger: logger,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("kafka consumer starting",
		slog.String("topic", c.reader.Config().Topic),
		slog.String("group_id", c.reader.Config().GroupID),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("kafka consumer context cancelled, exiting loop")
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Error("failed to fetch message from kafka", slog.Any("error", err))
				continue
			}

			var job Job
			if err := json.Unmarshal(msg.Value, &job); err != nil {
				c.logger.Error("malformed job message payload",
					slog.String("key", string(msg.Key)),
					slog.Any("error", err),
				)
				// Commit malformed messages immediately so invalid payloads don't poison the queue
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			// Attach Ack callback: Commits Kafka offset after worker successfully completes processing
			msgToCommit := msg
			job.Ack = func(ackCtx context.Context) error {
				return c.reader.CommitMessages(ackCtx, msgToCommit)
			}

			if err := c.pool.Submit(ctx, job); err != nil {
				c.logger.Error("worker pool rejected job submission",
					slog.String("job_id", job.ID),
					slog.Any("error", err),
				)
				continue
			}
		}
	}
}

func (c *Consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
