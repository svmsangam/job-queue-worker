package worker

import (
	"context"
	"fmt"
)

// Job represents a unit of work ingested from Kafka or the API.
type Job struct {
	ID      string
	Type    string
	Payload []byte
	Ack     func(ctx context.Context) error `json:"-"`
}

// JobProcessor defines the strategy interface for processing jobs.
type JobProcessor interface {
	Process(ctx context.Context, job Job) error
}

// gRPCJobProcessor implements JobProcessor by delegating to our gRPC client.
type gRPCJobProcessor struct {
	client *ProcessorClient
}

func NewGRPCJobProcessor(client *ProcessorClient) JobProcessor {
	return &gRPCJobProcessor{client: client}
}

func (p *gRPCJobProcessor) Process(ctx context.Context, job Job) error {
	resp, err := p.client.Process(ctx, job.ID, job.Payload, 2*1000000000) // 2s timeout
	if err != nil {
		return fmt.Errorf("grpc strategy execution failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("job failed upstream: %s", resp.ErrorMessage)
	}

	return nil
}
