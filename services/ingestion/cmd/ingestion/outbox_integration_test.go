package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
