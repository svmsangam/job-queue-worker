// Package worker owns the asynchronous job execution pipeline. It connects
// Kafka-delivered jobs to processing strategies and completion acknowledgments.
package worker

import (
	"context"
	"fmt"
)

// Job represents a unit of work ingested from Kafka or the API. Ack is kept out
// of JSON so transport-specific offset commits cannot leak into job payloads.
type Job struct {
	ID      string
	Type    string
	Payload []byte
	Ack     func(ctx context.Context) error `json:"-"`
}

// JobProcessor defines the strategy interface for processing jobs. The context
// carries cancellation from worker shutdown into the downstream processor.
type JobProcessor interface {
	Process(ctx context.Context, job Job) error
}

// gRPCJobProcessor implements JobProcessor by delegating to the processor
// service over gRPC.
type gRPCJobProcessor struct {
	client *ProcessorClient
}

// NewGRPCJobProcessor adapts a reusable ProcessorClient to JobProcessor.
func NewGRPCJobProcessor(client *ProcessorClient) JobProcessor {
	return &gRPCJobProcessor{client: client}
}

// Process sends one worker job to gRPC and converts an unsuccessful response
// into an error so the pool's retry and DLQ policy can handle it.
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
