package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const outboxBatchSize = 50

// publishBatch locks a batch of pending events, publishes
// them to Kafka, and records their broker acknowledgments.
//
// If the DB transaction rolls back after Kafka accepts the
// records, a subsequent attempt can publish duplicates.
// Consumers must therefore deduplicate by transaction ID.
func publishBatch(
	ctx context.Context,
	db *pgxpool.Pool,
	kafka *kgo.Client,
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
		FOR UPDATE SKIP LOCKED
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

func runPublisher(
	ctx context.Context,
	db *pgxpool.Pool,
	kafka *kgo.Client,
) {
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(
			ctx, 10*time.Second,
		)

		count, err := publishBatch(batchCtx, db, kafka)
		cancel()

		if err != nil && ctx.Err() == nil {
			log.Printf("outbox publisher: %v", err)
		}

		if count == outboxBatchSize {
			continue // More records may be waiting.
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
	}
}
