// Package metrics defines the process-local Prometheus collectors shared by
// API and worker services for queue depth, throughput, failures, and latency.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics contains job and worker metrics. Prometheus collectors are safe for
// concurrent updates, allowing every worker goroutine to record directly.
type Metrics struct {
	Registry      *prometheus.Registry
	JobProcessed  prometheus.Counter
	JobFailed     prometheus.Counter
	JobDuration   prometheus.Counter
	QueueDepth    prometheus.Gauge
	ActiveWorkers prometheus.Gauge
}

// New creates an isolated registry so tests and separate services do not share
// global collectors or produce duplicate-registration panics.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		Registry: registry,
		JobProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "job_processed_total", Help: "Total number of jobs completed successfully.",
		}),
		JobFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "job_failed_total", Help: "Total number of jobs that failed permanently.",
		}),
		JobDuration: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "job_duration_seconds_total", Help: "Total time spent processing jobs in seconds.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "job_queue_depth", Help: "Number of jobs waiting in the worker queue.",
		}),
		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "active_workers", Help: "Number of workers currently processing jobs.",
		}),
	}
	registry.MustRegister(metrics.JobProcessed, metrics.JobFailed, metrics.JobDuration, metrics.QueueDepth, metrics.ActiveWorkers)
	return metrics
}
