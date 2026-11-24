package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	outboxBatchSize         = 50
	outboxPublisherLockName = "fraudwatch:outbox-publisher:v1"
	outboxPublisherRetry    = 300 * time.Millisecond
)

type transactionBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

type syncProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// publishBatch locks a batch of pending events, publishes
// them to Kafka, and records their broker acknowledgments.
//
// If the DB transaction rolls back after Kafka accepts the
// records, a subsequent attempt can publish duplicates.
// Consumers must therefore deduplicate by transaction ID.
func publishBatch(
	ctx context.Context,
	db transactionBeginner,
	kafka syncProducer,
) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	rows, err := tx.Query(ctx, `
		SELECT transaction_id, kafka_key, payload
		FROM outbox_events
		WHERE delivered_at IS NULL
		ORDER BY created_at, transaction_id
		LIMIT $1
		FOR UPDATE
	`, outboxBatchSize)
	if err != nil {
		return 0, err
	}

	var ids []string
	var records []*kgo.Record

	for rows.Next() {
		var id, key string
		var payload []byte

		if err := rows.Scan(&id, &key, &payload); err != nil {
			rows.Close()
			return 0, err
		}

		ids = append(ids, id)
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: payload,
		})
	}

	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}

	if len(records) == 0 {
		return 0, tx.Commit(ctx)
	}

	if err := kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("Kafka publish failed: %w", err)
	}

	for i, record := range records {
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET delivered_at = now(),
			    kafka_partition = $2,
			    kafka_offset = $3
			WHERE transaction_id = $1
		`, ids[i], record.Partition, record.Offset)
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(records), nil
}

func tryPublisherLeadership(
	ctx context.Context,
	conn *pgx.Conn,
) (bool, error) {
	// This is a session lock. The caller must publish through this same
	// connection and close it on every exit so PostgreSQL releases leadership.
	var acquired bool
	err := conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock(hashtextextended($1, 0))
	`, outboxPublisherLockName).Scan(&acquired)
	return acquired, err
}

func runPublisher(
	ctx context.Context,
	databaseURL string,
	kafka syncProducer,
	metrics *ingestionMetrics,
) {
	for ctx.Err() == nil {
		// Keep leadership outside the API pool. A dedicated session makes lock
		// loss and transaction loss the same failure boundary and cannot starve
		// HTTP requests when a small pool is configured.
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			slog.Error("outbox leadership connection failed", "error", err, "outcome", "retry")
		} else {
			runPublisherSession(ctx, conn, kafka, metrics)
			closeCtx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			if err := conn.Close(closeCtx); err != nil {
				slog.Error("outbox leadership connection close failed", "error", err)
			}
			cancel()
		}

		if !waitForPublisherRetry(ctx) {
			return
		}
	}
}

func runPublisherSession(
	ctx context.Context,
	conn *pgx.Conn,
	kafka syncProducer,
	metrics *ingestionMetrics,
) {
	for ctx.Err() == nil {
		acquired, err := tryPublisherLeadership(ctx, conn)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("outbox leadership check failed", "error", err, "outcome", "reconnect")
			}
			return
		}
		if acquired {
			slog.Info("outbox publisher leadership acquired")
			runPublisherLeader(ctx, conn, kafka, metrics)
			return
		}
		if !waitForPublisherRetry(ctx) {
			return
		}
	}
}

func runPublisherLeader(
	ctx context.Context,
	conn *pgx.Conn,
	kafka syncProducer,
	metrics *ingestionMetrics,
) {
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(
			ctx, 10*time.Second,
		)

		started := time.Now()
		count, err := publishBatch(batchCtx, conn, kafka)
		cancel()
		elapsed := time.Since(started)
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			metrics.observeOutbox("failure", 0, elapsed)
			slog.Error("outbox publication failed", "error", err, "outcome", "retry", "duration_ms", elapsed.Milliseconds())
			if conn.IsClosed() {
				return
			}
		} else if count > 0 {
			metrics.observeOutbox("success", count, elapsed)
			slog.Info("outbox batch published", "count", count, "outcome", "published", "duration_ms", elapsed.Milliseconds())
		} else {
			metrics.observeOutbox("empty", 0, elapsed)
		}

		if count == outboxBatchSize {
			continue // More records may be waiting.
		}

		if !waitForPublisherRetry(ctx) {
			return
		}
	}
}

func waitForPublisherRetry(ctx context.Context) bool {
	timer := time.NewTimer(outboxPublisherRetry)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
