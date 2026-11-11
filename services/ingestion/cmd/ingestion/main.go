package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topic = "transactions.v1"

var ErrConflict = errors.New("transaction ID already has a different payload")

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	CustomerID    string  `json:"customer_id"`
	TerminalID    string  `json:"terminal_id"`
	Amount        float64 `json:"amount"`
	OccurredAt    string  `json:"occurred_at"`
}

type Server struct {
	db *pgxpool.Pool
}

func validate(tx *Transaction) error {
	if strings.TrimSpace(tx.TransactionID) == "" ||
		strings.TrimSpace(tx.CustomerID) == "" ||
		strings.TrimSpace(tx.TerminalID) == "" {
		return errors.New("transaction, customer and terminal IDs are required")
	}

	if tx.Amount < 0 || math.IsNaN(tx.Amount) ||
		math.IsInf(tx.Amount, 0) {
		return errors.New("amount must be finite and non-negative")
	}

	t, err := time.Parse(time.RFC3339Nano, tx.OccurredAt)
	if err != nil {
		return errors.New("occurred_at must be RFC3339")
	}

	tx.OccurredAt = t.UTC().Format(time.RFC3339Nano)
	return nil
}

// acceptTransaction returns duplicate=true for an identical replay.
// The transaction and its outbox event commit atomically.
func acceptTransaction(
	ctx context.Context,
	db *pgxpool.Pool,
	event Transaction,
) (duplicate bool, err error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(payload)

	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())

	var inserted string
	err = tx.QueryRow(ctx, `
		INSERT INTO transactions
		    (transaction_id, payload_hash, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (transaction_id) DO NOTHING
		RETURNING transaction_id
	`, event.TransactionID, digest[:], payload).Scan(&inserted)

	if errors.Is(err, pgx.ErrNoRows) {
		var storedHash []byte

		err = tx.QueryRow(ctx, `
			SELECT payload_hash
			FROM transactions
			WHERE transaction_id = $1
		`, event.TransactionID).Scan(&storedHash)
		if err != nil {
			return false, err
		}

		if !bytes.Equal(storedHash, digest[:]) {
			return false, ErrConflict
		}

		return true, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events
		    (transaction_id, topic, kafka_key, payload)
		VALUES ($1, $2, $3, $4)
	`, event.TransactionID, topic, event.TerminalID, payload)
	if err != nil {
		return false, err
	}

	return false, tx.Commit(ctx)
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()

	var event Transaction
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "unexpected trailing JSON", http.StatusBadRequest)
		return
	}

	if err := validate(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	duplicate, err := acceptTransaction(ctx, s.db, event)
	if errors.Is(err, ErrConflict) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		log.Printf("transaction acceptance failed: %v", err)
		http.Error(w, "transaction acceptance unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"transaction_id": event.TransactionID,
		"status":         "accepted",
		"duplicate":      duplicate,
	})
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://fraudwatch:fraudwatch_dev@" +
			"127.0.0.1:5434/fraudwatch?sslmode=disable"
	}

	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	pingCtx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	err = db.Ping(pingCtx)
	cancel()
	if err != nil {
		log.Fatalf("PostgreSQL unavailable: %v", err)
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kafka.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Publication failures do not prevent HTTP acceptance.
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		runPublisher(ctx, db, kafka)
	}()

	server := &Server{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/transactions", server.ingest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	httpServer := &http.Server{
		Addr:              fraudwatchHTTPAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
	}

	go func() {
		log.Printf("Ingestion API listening on %s", fraudwatchHTTPAddr())
		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server failure: %v", err)
			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	<-publisherDone
}

// fraudwatchHTTPAddr preserves the loopback-only development default and lets
// the container bind to 0.0.0.0 via FRAUDWATCH_HTTP_ADDR.
func fraudwatchHTTPAddr() string {
	if addr := os.Getenv("FRAUDWATCH_HTTP_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:8080"
}
