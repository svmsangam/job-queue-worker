// Package main exposes the gRPC processor service. It validates requests,
// performs the domain work, and reports structured request outcomes.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "job-queue/api/proto/v1"
	"job-queue/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// processorServer implements the generated ProcessorServiceServer interface.
// Worker pool -> gRPC request -> processorServer -> processing response.
type processorServer struct {
	pb.UnimplementedProcessorServiceServer
	logger *slog.Logger
}

// newProcessorServer wires logging into the gRPC service implementation.
func newProcessorServer(logger *slog.Logger) *processorServer {
	return &processorServer{logger: logger}
}

// ProcessJob handles incoming processing requests after checking cancellation
// and required identifiers. Worker gRPC call -> validation/work -> response.
func (s *processorServer) ProcessJob(ctx context.Context, req *pb.ProcessRequest) (*pb.ProcessResponse, error) {
	// 1. Enforce context deadline check
	select {
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "request canceled or timed out")
	default:
	}

	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}

	s.logger.Info("processing job payload",
		slog.String("job_id", req.GetJobId()),
		slog.Int("payload_size", len(req.GetPayload())),
	)

	// Simulate work duration
	time.Sleep(100 * time.Millisecond)

	// return &pb.ProcessResponse{
	// 	Success:      false,
	// 	Output:       fmt.Sprintf("failed to process job %s", req.GetJobId()),
	// 	ErrorMessage: "",
	// }, errors.New("simulated processing failure")

	return &pb.ProcessResponse{
		Success:      true,
		Output:       fmt.Sprintf("successfully processed job %s", req.GetJobId()),
		ErrorMessage: "",
	}, nil
}

// loggingInterceptor records gRPC method, status, error, and duration around
// each handler without changing the handler response.
func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		// Execute handler
		resp, err := handler(ctx, req)

		duration := time.Since(start)
		stCode := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				stCode = st.Code()
			} else {
				stCode = codes.Unknown
			}
		}

		logger.Info("gRPC request handled",
			slog.String("method", info.FullMethod),
			slog.String("status_code", stCode.String()),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)

		return resp, err
	}
}

// main starts the gRPC processor and uses a buffered signal channel to receive
// one OS shutdown notification before graceful server termination.
func main() {
	logHandler, err := logger.New(logger.Config{LokiURL: "http://localhost:3100/loki/api/v1/push", Service: "processor", Level: slog.LevelInfo})
	if err != nil {
		panic(err)
	}
	defer logHandler.Close(context.Background())
	logger := slog.New(logHandler)

	port := ":50051"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		logger.Error("failed to listen", slog.String("port", port), slog.Any("error", err))
		os.Exit(1)
	}

	// Register gRPC server with interceptor middleware
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor(logger)),
	)

	server := newProcessorServer(logger)
	pb.RegisterProcessorServiceServer(grpcServer, server)

	// Graceful shutdown listener
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting gRPC processor service", slog.String("addr", port))
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("gRPC server error", slog.Any("error", err))
		}
	}()

	<-stopChan
	logger.Info("shutting down gRPC server gracefully...")
	grpcServer.GracefulStop()
	logger.Info("server stopped")
}
