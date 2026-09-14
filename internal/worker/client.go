package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "job-queue/api/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ProcessorClient struct {
	conn   *grpc.ClientConn
	client pb.ProcessorServiceClient
	logger *slog.Logger
}

// NewProcessorClient establishes a reusable gRPC connection pool.
func NewProcessorClient(target string, logger *slog.Logger) (*ProcessorClient, error) {
	// Configure transport credentials (insecure for internal microservice communication)
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC target %s: %w", target, err)
	}

	return &ProcessorClient{
		conn:   conn,
		client: pb.NewProcessorServiceClient(conn),
		logger: logger,
	}, nil
}

// Process executes the gRPC call with a strict context deadline.
func (c *ProcessorClient) Process(ctx context.Context, jobID string, payload []byte, timeout time.Duration) (*pb.ProcessResponse, error) {
	// Enforce strict timeout per outbound request
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := &pb.ProcessRequest{
		JobId:          jobID,
		Payload:        payload,
		TimeoutSeconds: int32(timeout.Seconds()),
	}

	resp, err := c.client.ProcessJob(ctxWithTimeout, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC process job failed: %w", err)
	}

	return resp, nil
}

// Close gracefully closes the underlying connection pool.
func (c *ProcessorClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
