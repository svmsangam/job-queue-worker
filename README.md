# Job Queue

Job Queue is an event-driven Go service for accepting jobs over HTTP and buffering them durably in Kafka. A dedicated worker pool consumes queued jobs, invokes a gRPC processor with bounded retries, acknowledges successful Kafka offsets, and routes exhausted failures to a dead-letter topic.

## High-Level Architecture & Data Flow

```text
                          Observability
                  +-----------------------------+
                  | Prometheus <- /metrics      |
                  | Loki <- async structured log|
                  | Grafana -> Prometheus/Loki  |
                  +-----------------------------+

Client / k6
    |
    | POST /v1/jobs {id, type, payload}
    v
+------------------+       synchronous keyed       +------------------+
| Go HTTP API      | -----------------------------> | Kafka            |
| :8080            |       publish to jobs.v1      | jobs.v1 topic    |
| validation       |                               +--------+---------+
+------------------+                                        |
                                                            | consumer group
                                                            | worker-group-1
                                                            v
                                                  +----------------------+
                                                  | Kafka Consumer       |
                                                  | manual offset commit|
                                                  +----------+-----------+
                                                             |
                                                             | bounded channel
                                                             | queue buffer: 50
                                                             v
                                                  +----------------------+
                                                  | Worker Pool          |
                                                  | concurrency: 3       |
                                                  | retries: 3           |
                                                  +----------+-----------+
                                                             |
                                      success                  | failure after retries
                                         |                     v
                                         |          +------------------------+
                                         |          | Kafka DLQ: jobs.dlq    |
                                         |          | original job + reason  |
                                         |          +------------------------+
                                         v
                              +----------------------+
                              | gRPC Processor       |
                              | :50051               |
                              | ProcessorService     |
                              +----------+-----------+
                                         |
                                         | successful response
                                         v
                              Kafka offset acknowledgment

Data path: HTTP client -> API validation -> Kafka jobs.v1 -> consumer -> buffered worker pool.
Processing path: worker goroutine -> gRPC processor -> success acknowledgment, or retry -> jobs.dlq.
```

### Architectural Patterns

- **Event-driven processing:** The HTTP API is an enqueue boundary. Kafka decouples request acceptance from processing capacity and provides durable delivery between services.
- **Producer/consumer messaging:** The API and producer command publish JSON job messages to `jobs.v1`. The worker uses Kafka consumer group `worker-group-1`.
- **Bounded worker pool:** The worker service starts three processing goroutines and a buffered job channel with capacity 50. A full queue applies backpressure to Kafka submission instead of creating unbounded in-memory work.
- **Manual acknowledgment:** Kafka automatic commits are disabled. The consumer attaches an acknowledgment callback to each job, and the worker commits the source offset only after successful processing. Malformed JSON is committed immediately so it cannot poison the queue.
- **Retry and dead-letter handling:** Processing is attempted up to three times with exponential backoff. A permanently failed job is synchronously published to `jobs.dlq` before the source offset can be acknowledged.
- **gRPC service boundary:** Workers reuse one multiplexed gRPC client connection to call `ProcessorService.ProcessJob`. Each call receives a two-second child deadline derived from the worker context.
- **Stateless API:** API instances hold no job state. Job state is represented by the Kafka record and its acknowledgment status, allowing the HTTP layer to scale independently of workers.
- **Graceful shutdown:** Context cancellation stops consumer polling, a shutdown channel signals the process, and the worker pool drains accepted jobs through a `sync.WaitGroup` before returning.
- **Observability fan-out:** Structured logs are sent to console and daily local files, with optional asynchronous Loki batching. Prometheus metrics are exposed through isolated registries by the API and worker services.

## Tech Stack & Infrastructure

### Languages and runtime

- Go `1.26.4`
- JavaScript for the optional k6 load test in `test_jobs.js`
- JSON for HTTP and Kafka job payloads
- Protocol Buffers (`proto3`) for the gRPC contract

### Application frameworks and libraries

- Go standard library: `net/http`, `context`, `encoding/json`, `log/slog`, `os/signal`, `sync`
- `github.com/segmentio/kafka-go` for Kafka readers, writers, topic administration, and message commits
- `google.golang.org/grpc` for the processor RPC service and client
- `google.golang.org/protobuf` for generated Protocol Buffer types
- `github.com/prometheus/client_golang` for metrics and HTTP exposition

### Messaging and protocols

- Apache Kafka for durable job queues and dead-letter delivery
- Zookeeper for the local Kafka deployment in Docker Compose
- REST-style HTTP/JSON endpoints for job submission and health checks
- gRPC with Protocol Buffers for worker-to-processor communication
- No GraphQL implementation is currently included

### Observability infrastructure

- Prometheus scrapes API metrics on `http://localhost:8080/metrics`
- Prometheus scrapes worker metrics on `http://localhost:2112/metrics`
- Loki receives asynchronous structured logs on `http://localhost:3100`
- Grafana is available at `http://localhost:3000`

### Data stores and frontend

- No SQL database, NoSQL database, Redis cache, or other persistent application database is configured.
- No frontend application or client UI library is included. Clients call the HTTP API directly.

## Key Engineering & Performance Highlights

- **Durable enqueue:** Kafka writes are synchronous, so the API returns `202 Accepted` only after the job has been handed to Kafka successfully. A job ID is used as the Kafka message key to support stable routing semantics for related jobs.
- **Controlled concurrency:** The worker service uses three goroutines in production startup, while the reusable `Pool` abstraction defaults to five. The queue buffer is explicitly bounded to 50 at service startup, limiting memory growth under load.
- **Backpressure:** `Pool.Submit` selects across the bounded job channel, caller context, and pool shutdown signal. Producers do not wait forever when the queue is saturated or the service is stopping.
- **Failure consistency:** Kafka offsets are not committed after processor failure, so unacknowledged work can be redelivered. Once retries are exhausted, the worker publishes a structured DLQ record containing the original job, error reason, timestamp, and attempt count.
- **Timeout propagation:** Request contexts flow from HTTP or service shutdown into Kafka operations, worker processing, and gRPC calls. The processor client adds a strict two-second per-call deadline.
- **Non-blocking remote logging:** Loki logging uses a bounded channel and non-blocking enqueue. If Loki is unavailable or the channel is full, application processing is not blocked by remote log delivery.
- **Concurrent metrics:** Prometheus counters and gauges are updated directly from worker goroutines using concurrency-safe collectors. Separate registries avoid duplicate registration across services and tests.
- **Load-test profile:** `test_jobs.js` uses k6 to issue 100 requests per second for 30 seconds, with 20 preallocated virtual users and a maximum of 50 virtual users.

## Project Layout

```text
job-queue/
├── api/
│   └── proto/v1/
│       ├── processor.proto              # gRPC service and message schema
│       ├── processor.pb.go               # Generated protobuf messages
│       └── processor_grpc.pb.go          # Generated gRPC client/server code
├── cmd/
│   ├── api/main.go                       # HTTP API on :8080
│   ├── processor/main.go                 # gRPC processor on :50051
│   ├── producer/main.go                  # Kafka batch producer example
│   └── worker/main.go                    # Worker, consumer, DLQ, metrics
├── internal/
│   ├── producer/
│   │   └── kafka.go                       # Synchronous Kafka producer
│   └── worker/
│       ├── client.go                      # Reusable gRPC processor client
│       ├── consumer.go                    # Kafka consumer and manual commits
│       ├── dlq.go                         # Dead-letter topic publisher
│       ├── processor.go                   # Job and processing strategy types
│       └── worker.go                      # Pool, retries, backpressure, metrics
├── pkg/
│   ├── logger/
│   │   ├── logger.go                      # slog fan-out, files, Loki batching
│   │   └── 18-09-2026.txt                 # Existing local log output
│   └── metrics/
│       └── metrics.go                     # Prometheus collectors and registry
├── docker-compose.yml                     # Kafka, Zookeeper, Loki, Prometheus, Grafana
├── prometheus.yml                         # API and worker scrape targets
├── request.json                            # Example job request body
├── test_jobs.js                            # Optional k6 load test
├── go.mod                                  # Go module and dependencies
└── go.sum                                  # Dependency checksums
```

## Getting Started

### Prerequisites

- Go `1.26.4` or a compatible Go toolchain
- Docker Engine and Docker Compose
- Optional: k6 for the load test
- Available local ports: `2181`, `3000`, `3100`, `50051`, `8080`, `9092`, `9190`, and `2112`

### 1. Start infrastructure

From the repository root:

```bash
docker compose up -d
```

This starts Zookeeper, Kafka, Loki, Prometheus, and Grafana. The Compose Kafka configuration advertises the broker as `localhost:9092`, which matches the host-run Go services.

Check container status:

```bash
docker compose ps
```

### 2. Download Go dependencies

```bash
go mod download
```

No environment variables are required by the current implementation. Service addresses, Kafka topics, and ports are defined in the command entry points and `prometheus.yml`.

### 3. Start the processor

In a terminal from the repository root:

```bash
go run ./cmd/processor
```

The gRPC processor listens on `localhost:50051`.

### 4. Start the worker

In a second terminal:

```bash
go run ./cmd/worker
```

The worker connects to Kafka, consumes `jobs.v1`, starts the processing pool, creates or verifies `jobs.dlq`, and exposes metrics on `localhost:2112/metrics`.

### 5. Start the HTTP API

In a third terminal:

```bash
go run ./cmd/api
```

The API listens on `localhost:8080`.

### 6. Submit a job

Health check:

```bash
curl http://localhost:8080/health
```

Submit a job:

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  --data '{"id":"job-123","type":"IMAGE_RESIZE","payload":{"image_url":"https://example.com/image.jpg"}}'
```

Expected response shape:

```json
{
  "jobid": "job-123",
  "status": "ENQUEUED",
  "timestamp": "2026-09-23T12:00:00Z"
}
```

The API accepts a maximum request body size of 1 MiB and requires `id`, `type`, and `payload`. Unknown JSON fields and multiple JSON objects in one request are rejected.

### 7. Optional producer command

The producer command publishes five example jobs to `jobs.v1`:

```bash
go run ./cmd/producer
```

### 8. Optional load test

With the API, worker, processor, and infrastructure running, install k6 and execute:

```bash
k6 run test_jobs.js
```

The test targets `http://localhost:8080/v1/jobs` at 100 requests per second for 30 seconds and checks for HTTP `202` responses.

### 9. Stop local services

Stop the Docker infrastructure with:

```bash
docker compose down
```

The Go services can be stopped with `Ctrl+C`; each service handles interrupt and termination signals for graceful shutdown.

## Service Endpoints and Topics

| Component | Address | Purpose |
| --- | --- | --- |
| HTTP API | `localhost:8080` | `GET /health`, `POST /v1/jobs`, `GET /metrics` |
| gRPC processor | `localhost:50051` | `processor.v1.ProcessorService/ProcessJob` |
| Worker metrics | `localhost:2112/metrics` | Prometheus metrics for worker processing |
| Prometheus | `localhost:9190` | Metrics query and scrape status |
| Grafana | `localhost:3000` | Dashboards and observability UI |
| Loki | `localhost:3100` | Structured log ingestion |
| Kafka | `localhost:9092` | Job broker |

| Kafka topic | Role |
| --- | --- |
| `jobs.v1` | Primary job queue |
| `jobs.dlq` | Failed jobs after retry exhaustion |
| `worker-group-1` | Worker consumer group |

## Development and Validation

Format the Go source:

```bash
gofmt -w ./cmd ./internal ./pkg
```

Run tests with reduced build parallelism on memory-constrained machines:

```bash
go test -p 1 ./...
```

Run static analysis:

```bash
go vet -p 1 ./...
```

The repository currently contains no Go test files; these commands primarily validate compilation, package loading, and static correctness.
