package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type blockingOrderProducer struct {
	mu             sync.Mutex
	firstStarted   chan struct{}
	firstRelease   chan struct{}
	releaseOnce    sync.Once
	calls          int
	nextOffset     int64
	publishedOrder []string
}

func newBlockingOrderProducer() *blockingOrderProducer {
	return &blockingOrderProducer{
		firstStarted: make(chan struct{}),
		firstRelease: make(chan struct{}),
	}
}

func (p *blockingOrderProducer) ProduceSync(
	ctx context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	p.mu.Lock()
	call := p.calls
	p.calls++
	if call == 0 {
		close(p.firstStarted)
	}
	p.mu.Unlock()

	if call == 0 {
		select {
		case <-ctx.Done():
			results := make(kgo.ProduceResults, len(records))
			for i, record := range records {
				results[i] = kgo.ProduceResult{
					Record: record,
					Err:    ctx.Err(),
				}
			}
			return results
		case <-p.firstRelease:
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		record.Partition = 0
		record.Offset = p.nextOffset
		p.nextOffset++
		p.publishedOrder = append(
			p.publishedOrder, string(record.Value),
		)
		results[i] = kgo.ProduceResult{Record: record}
	}
	return results
}

func (p *blockingOrderProducer) releaseFirstBatch() {
	p.releaseOnce.Do(func() { close(p.firstRelease) })
}

func (p *blockingOrderProducer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *blockingOrderProducer) order() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.publishedOrder...)
}

func TestOutboxIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := fmt.Sprintf("integration-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		c, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		_, _ = db.Exec(c,
			"DELETE FROM outbox_events WHERE transaction_id=$1", id)
		_, _ = db.Exec(c,
			"DELETE FROM transactions WHERE transaction_id=$1", id)
	})

	server := &Server{db: db}

	send := func(amount float64) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{
			"transaction_id": %q,
			"customer_id": "596",
			"terminal_id": "3156",
			"amount": %v,
			"occurred_at": "2018-04-01T00:00:31Z"
		}`, id, amount)

		req := httptest.NewRequest(
			http.MethodPost,
			"/v1/transactions",
			strings.NewReader(body),
		)

		resp := httptest.NewRecorder()
		server.ingest(resp, req)
		return resp
	}

	first := send(57.16)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first request: %d %s",
			first.Code, first.Body.String())
	}

	var firstAck struct {
		Duplicate bool `json:"duplicate"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstAck); err != nil {
		t.Fatal(err)
	}
	if firstAck.Duplicate {
		t.Fatal("first request must not be a duplicate")
	}

	second := send(57.16)
	if second.Code != http.StatusAccepted {
		t.Fatalf("retry: %d %s",
			second.Code, second.Body.String())
	}

	var secondAck struct {
		Duplicate bool `json:"duplicate"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondAck); err != nil {
		t.Fatal(err)
	}
	if !secondAck.Duplicate {
		t.Fatal("identical retry must be a duplicate")
	}

	conflict := send(99.99)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting request: got %d, want 409",
			conflict.Code)
	}

	var eventCount int
	err = db.QueryRow(ctx, `
		SELECT count(*)
		FROM outbox_events
		WHERE transaction_id=$1
	`, id).Scan(&eventCount)
	if err != nil || eventCount != 1 {
		t.Fatalf("expected one outbox event: %d %v",
			eventCount, err)
	}

	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(3, topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	count, err := publishBatch(ctx, db, client)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("published %d records, expected 1", count)
	}

	var delivered bool
	var partition int32
	var offset int64

	err = db.QueryRow(ctx, `
		SELECT delivered_at IS NOT NULL,
		       kafka_partition, kafka_offset
		FROM outbox_events
		WHERE transaction_id=$1
	`, id).Scan(&delivered, &partition, &offset)
	if err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("outbox event was not marked delivered")
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("Kafka consume error: %v", errs)
	}

	var found bool
	fetches.EachRecord(func(record *kgo.Record) {
		var event Transaction
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Errorf("invalid event: %v", err)
			return
		}

		if event.TransactionID == id {
			found = true
			if string(record.Key) != "3156" {
				t.Errorf("unexpected Kafka key: %s", record.Key)
			}
			if record.Partition != partition ||
				record.Offset != offset {
				t.Error("Kafka coordinates differ from outbox")
			}
		}
	})

	if !found {
		t.Fatal("published event not found in Kafka")
	}

	t.Log("PostgreSQL idempotency and Kafka publication: PASSED")
}

func TestOverlappingPublishersPreserveBatchOrder(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	schema := fmt.Sprintf("outbox_order_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()
		_, _ = admin.Exec(
			cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE",
		)
	})
	if _, err := admin.Exec(ctx, `
		CREATE TABLE `+quotedSchema+`.outbox_events (
			transaction_id TEXT PRIMARY KEY,
			kafka_key TEXT NOT NULL,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			delivered_at TIMESTAMPTZ,
			kafka_partition INTEGER,
			kafka_offset BIGINT
		)
	`); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	runID := time.Now().UnixNano()
	ids := make([]string, outboxBatchSize+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("ordered-%d-%02d", runID, i)
		_, err := admin.Exec(ctx, `
			INSERT INTO `+quotedSchema+`.outbox_events (
				transaction_id, kafka_key, payload, created_at
			) VALUES ($1, $2, $3, $4)
		`, ids[i], "shared-terminal", []byte(ids[i]), baseTime.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	connect := func() *pgx.Conn {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(
			ctx, "SET search_path TO "+quotedSchema,
		); err != nil {
			_ = conn.Close(ctx)
			t.Fatal(err)
		}
		return conn
	}
	leader := connect()
	defer leader.Close(context.Background())
	standby := connect()
	defer standby.Close(context.Background())

	acquired, err := tryPublisherLeadership(ctx, leader)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("first publisher did not acquire leadership")
	}

	producer := newBlockingOrderProducer()
	type result struct {
		count int
		err   error
	}
	firstDone := make(chan result, 1)
	firstFinished := make(chan struct{})
	go func() {
		defer close(firstFinished)
		count, err := publishBatch(ctx, leader, producer)
		firstDone <- result{count: count, err: err}
	}()
	defer func() {
		producer.releaseFirstBatch()
		select {
		case <-firstFinished:
		case <-time.After(2 * time.Second):
		}
	}()

	select {
	case <-producer.firstStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	acquired, err = tryPublisherLeadership(ctx, standby)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("overlapping publisher acquired leadership")
	}
	if producer.callCount() != 1 {
		t.Fatalf(
			"standby published during the leader batch: calls=%d",
			producer.callCount(),
		)
	}

	producer.releaseFirstBatch()
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	if first.count != outboxBatchSize {
		t.Fatalf(
			"first publisher sent %d records, expected %d",
			first.count, outboxBatchSize,
		)
	}
	if err := leader.Close(ctx); err != nil {
		t.Fatal(err)
	}

	acquired, err = tryPublisherLeadership(ctx, standby)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("standby did not take leadership after leader exit")
	}
	count, err := publishBatch(ctx, standby, producer)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("standby sent %d records, expected 1", count)
	}

	published := producer.order()
	if len(published) != len(ids) {
		t.Fatalf("published %d records, expected %d", len(published), len(ids))
	}
	for i := range ids {
		if published[i] != ids[i] {
			t.Fatalf(
				"publish order[%d]=%s, expected %s",
				i, published[i], ids[i],
			)
		}
	}

	rows, err := admin.Query(ctx, `
		SELECT transaction_id, delivered_at IS NOT NULL,
		       kafka_partition, kafka_offset
		FROM `+quotedSchema+`.outbox_events
		ORDER BY created_at, transaction_id
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		var id string
		var delivered bool
		var partition int32
		var offset int64
		if err := rows.Scan(&id, &delivered, &partition, &offset); err != nil {
			t.Fatal(err)
		}
		if id != ids[i] || !delivered || partition != 0 || offset != int64(i) {
			t.Fatalf(
				"row %d mismatch: id=%s delivered=%t partition=%d offset=%d",
				i, id, delivered, partition, offset,
			)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
