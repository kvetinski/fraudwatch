package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var requestDurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
var outboxDurationBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type localHistogram struct {
	buckets []float64
	counts  []uint64
	count   uint64
	sum     float64
}

func newLocalHistogram(buckets []float64) localHistogram {
	return localHistogram{buckets: buckets, counts: make([]uint64, len(buckets))}
}

func (h *localHistogram) observe(value float64) {
	h.count++
	h.sum += value
	for index, upper := range h.buckets {
		if value <= upper {
			h.counts[index]++
		}
	}
}

type ingestionMetrics struct {
	mu sync.Mutex

	requests          map[string]uint64
	requestDuration   localHistogram
	outboxAttempts    map[string]uint64
	outboxPublished   uint64
	outboxDuration    localHistogram
	outboxLastSuccess float64
	buildRevision     string
}

func newIngestionMetrics(buildRevision string) *ingestionMetrics {
	return &ingestionMetrics{
		requests: map[string]uint64{
			"accepted":    0,
			"duplicate":   0,
			"invalid":     0,
			"conflict":    0,
			"unavailable": 0,
		},
		requestDuration: newLocalHistogram(requestDurationBuckets),
		outboxAttempts: map[string]uint64{
			"success": 0,
			"failure": 0,
			"empty":   0,
		},
		outboxDuration: newLocalHistogram(outboxDurationBuckets),
		buildRevision:  buildRevision,
	}
}

func (m *ingestionMetrics) observeRequest(outcome string, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[outcome]++
	m.requestDuration.observe(elapsed.Seconds())
}

func (m *ingestionMetrics) observeOutbox(result string, count int, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outboxAttempts[result]++
	m.outboxDuration.observe(elapsed.Seconds())
	if result == "success" {
		m.outboxPublished += uint64(count)
		m.outboxLastSuccess = float64(time.Now().Unix())
	}
}

func renderHistogram(builder *strings.Builder, name, help string, histogram localHistogram) {
	fmt.Fprintf(builder, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for index, upper := range histogram.buckets {
		fmt.Fprintf(builder, "%s_bucket{le=\"%g\"} %d\n", name, upper, histogram.counts[index])
	}
	fmt.Fprintf(builder, "%s_bucket{le=\"+Inf\"} %d\n", name, histogram.count)
	fmt.Fprintf(builder, "%s_sum %g\n%s_count %d\n", name, histogram.sum, name, histogram.count)
}

func escapePrometheusLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func (m *ingestionMetrics) handler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var pending int64
		var oldest float64
		snapshotOK := 1
		if err := db.QueryRow(ctx, `
			SELECT count(*), COALESCE(
				EXTRACT(EPOCH FROM now() - min(created_at)), 0
			)
			FROM outbox_events WHERE delivered_at IS NULL
		`).Scan(&pending, &oldest); err != nil {
			snapshotOK = 0
			pending = 0
			oldest = 0
		}

		m.mu.Lock()
		requests := make(map[string]uint64, len(m.requests))
		for key, value := range m.requests {
			requests[key] = value
		}
		attempts := make(map[string]uint64, len(m.outboxAttempts))
		for key, value := range m.outboxAttempts {
			attempts[key] = value
		}
		requestDuration := m.requestDuration
		requestDuration.counts = append([]uint64(nil), m.requestDuration.counts...)
		outboxDuration := m.outboxDuration
		outboxDuration.counts = append([]uint64(nil), m.outboxDuration.counts...)
		published := m.outboxPublished
		lastSuccess := m.outboxLastSuccess
		m.mu.Unlock()

		var builder strings.Builder
		builder.WriteString("# HELP fraudwatch_ingestion_build_info Build identity for the ingestion service.\n# TYPE fraudwatch_ingestion_build_info gauge\n")
		fmt.Fprintf(&builder, "fraudwatch_ingestion_build_info{revision=\"%s\"} 1\n", escapePrometheusLabel(m.buildRevision))
		builder.WriteString("# HELP fraudwatch_ingestion_requests_total HTTP transaction-ingestion requests by durable outcome.\n# TYPE fraudwatch_ingestion_requests_total counter\n")
		outcomes := make([]string, 0, len(requests))
		for outcome := range requests {
			outcomes = append(outcomes, outcome)
		}
		sort.Strings(outcomes)
		for _, outcome := range outcomes {
			fmt.Fprintf(&builder, "fraudwatch_ingestion_requests_total{outcome=\"%s\"} %d\n", outcome, requests[outcome])
		}
		renderHistogram(&builder, "fraudwatch_ingestion_request_duration_seconds", "Transaction request duration through the PostgreSQL commit boundary.", requestDuration)
		builder.WriteString("# HELP fraudwatch_ingestion_outbox_publish_attempts_total Outbox publication attempts by result.\n# TYPE fraudwatch_ingestion_outbox_publish_attempts_total counter\n")
		results := make([]string, 0, len(attempts))
		for result := range attempts {
			results = append(results, result)
		}
		sort.Strings(results)
		for _, result := range results {
			fmt.Fprintf(&builder, "fraudwatch_ingestion_outbox_publish_attempts_total{result=\"%s\"} %d\n", result, attempts[result])
		}
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_outbox_published_events_total Events acknowledged by Kafka and marked delivered.\n# TYPE fraudwatch_ingestion_outbox_published_events_total counter\nfraudwatch_ingestion_outbox_published_events_total %d\n", published)
		renderHistogram(&builder, "fraudwatch_ingestion_outbox_publish_duration_seconds", "Outbox batch publication duration through the PostgreSQL commit boundary.", outboxDuration)
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_outbox_last_success_unixtime Unix time of the last non-empty successful outbox batch.\n# TYPE fraudwatch_ingestion_outbox_last_success_unixtime gauge\nfraudwatch_ingestion_outbox_last_success_unixtime %g\n", lastSuccess)
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_outbox_pending_events Pending outbox records.\n# TYPE fraudwatch_ingestion_outbox_pending_events gauge\nfraudwatch_ingestion_outbox_pending_events %d\n", pending)
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_outbox_oldest_pending_age_seconds Age of the oldest pending outbox record.\n# TYPE fraudwatch_ingestion_outbox_oldest_pending_age_seconds gauge\nfraudwatch_ingestion_outbox_oldest_pending_age_seconds %g\n", oldest)
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_monitor_snapshot_success Whether the outbox database snapshot succeeded.\n# TYPE fraudwatch_ingestion_monitor_snapshot_success gauge\nfraudwatch_ingestion_monitor_snapshot_success %d\n", snapshotOK)

		stat := db.Stat()
		builder.WriteString("# HELP fraudwatch_ingestion_db_pool_connections PostgreSQL pool connections by state.\n# TYPE fraudwatch_ingestion_db_pool_connections gauge\n")
		fmt.Fprintf(&builder, "fraudwatch_ingestion_db_pool_connections{state=\"acquired\"} %d\n", stat.AcquiredConns())
		fmt.Fprintf(&builder, "fraudwatch_ingestion_db_pool_connections{state=\"idle\"} %d\n", stat.IdleConns())
		fmt.Fprintf(&builder, "fraudwatch_ingestion_db_pool_connections{state=\"constructing\"} %d\n", stat.ConstructingConns())
		fmt.Fprintf(&builder, "fraudwatch_ingestion_db_pool_connections{state=\"total\"} %d\n", stat.TotalConns())
		fmt.Fprintf(&builder, "# HELP fraudwatch_ingestion_db_pool_max_connections Configured PostgreSQL pool limit.\n# TYPE fraudwatch_ingestion_db_pool_max_connections gauge\nfraudwatch_ingestion_db_pool_max_connections %d\n", stat.MaxConns())

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(builder.String()))
	}
}
