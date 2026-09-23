// Package main exposes the HTTP ingress service. It validates JSON job
// requests, publishes them to Kafka, and exposes health and Prometheus routes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"job-queue/internal/producer"
	"job-queue/pkg/logger"
	"job-queue/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	kafkaBroker = "localhost:9092"
	kafkaTopic  = "jobs.v1"
	listenAddr  = ":8080"
)

type jobRequest struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type jobResponse struct {
	JobID     string    `json:"jobid"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type apiServer struct {
	producer *producer.KafkaProducer
	logger   *slog.Logger
}

// handleHealth reports liveness without touching the Kafka dependency.
func (s *apiServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCreateJob validates an HTTP request and enqueues it. HTTP JSON request
// -> keyed Kafka message -> worker consumer; the response confirms enqueue.
func (s *apiServer) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var request jobRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid job request: "+err.Error())
		return
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}

	request.ID = strings.TrimSpace(request.ID)
	request.Type = strings.TrimSpace(request.Type)
	if request.ID == "" || request.Type == "" {
		writeError(w, http.StatusBadRequest, "id and type are required")
		return
	}
	if request.Payload == nil {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	jobData := map[string]interface{}{
		"id":      request.ID,
		"type":    request.Type,
		"payload": []byte(request.Payload),
	}

	message, err := json.Marshal(jobData)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode job")
		return
	}

	if err := s.producer.PublishJob(r.Context(), request.ID, message); err != nil {
		s.logger.Error("failed to enqueue job", slog.String("job_id", request.ID), slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	writeJSON(w, http.StatusAccepted, jobResponse{
		JobID:     request.ID,
		Status:    "ENQUEUED",
		Timestamp: time.Now(),
	})
}

// writeJSON encodes a response with the service's JSON content type.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError returns a consistent JSON error envelope.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// main starts the HTTP API and metrics endpoint, then gracefully shuts them
// down when the process receives SIGINT or SIGTERM.
func main() {
	logHandler, err := logger.New(logger.Config{LokiURL: "http://localhost:3100/loki/api/v1/push", Service: "api", Level: slog.LevelInfo})
	if err != nil {
		panic(err)
	}
	defer logHandler.Close(context.Background())
	logger := slog.New(logHandler)
	serviceMetrics := metrics.New()

	jobProducer := producer.NewKafkaProducer([]string{kafkaBroker}, kafkaTopic, logger)
	defer jobProducer.Close()

	jobAPI := &apiServer{producer: jobProducer, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", jobAPI.handleHealth)
	mux.HandleFunc("POST /v1/jobs", jobAPI.handleCreateJob)
	mux.Handle("GET /metrics", promhttp.HandlerFor(serviceMetrics.Registry, promhttp.HandlerOpts{}))

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("api server listening", slog.String("address", listenAddr))
		serverErr <- server.ListenAndServe()
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server stopped unexpectedly", slog.Any("error", err))
			os.Exit(1)
		}
	case <-shutdown:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("failed to shut down api server", slog.Any("error", err))
		}
	}
}
