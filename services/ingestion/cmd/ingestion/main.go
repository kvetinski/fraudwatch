package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic         = "transactions.v1"
	maxEventBytes = 64 << 10
)

var ErrConflict = errors.New("transaction ID already has a different payload")

var buildRevision = "development"

var strictRFC3339 = regexp.MustCompile(
	`^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T` +
		`([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]` +
		`(\.[0-9]{1,9})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`,
)

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	CustomerID    string  `json:"customer_id"`
	TerminalID    string  `json:"terminal_id"`
	Amount        float64 `json:"amount"`
	OccurredAt    string  `json:"occurred_at"`
}

type transactionEnvelope struct {
	TransactionID string   `json:"transaction_id"`
	CustomerID    string   `json:"customer_id"`
	TerminalID    string   `json:"terminal_id"`
	Amount        *float64 `json:"amount"`
	OccurredAt    string   `json:"occurred_at"`
}

type Server struct {
	db      *pgxpool.Pool
	metrics *ingestionMetrics
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

	if !strictRFC3339.MatchString(tx.OccurredAt) {
		return errors.New("occurred_at must use strict RFC3339")
	}
	t, err := time.Parse(time.RFC3339Nano, tx.OccurredAt)
	if err != nil {
		return errors.New("occurred_at must be RFC3339")
	}

	tx.OccurredAt = t.UTC().Format(time.RFC3339Nano)
	return nil
}

func decodeTransaction(body io.Reader) (Transaction, error) {
	var envelope transactionEnvelope
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&envelope); err != nil {
		return Transaction{}, errors.New("invalid transaction JSON")
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Transaction{}, errors.New("unexpected trailing JSON")
	}

	if envelope.Amount == nil {
		return Transaction{}, errors.New("amount is required and must not be null")
	}

	event := Transaction{
		TransactionID: envelope.TransactionID,
		CustomerID:    envelope.CustomerID,
		TerminalID:    envelope.TerminalID,
		Amount:        *envelope.Amount,
		OccurredAt:    envelope.OccurredAt,
	}
	if err := validate(&event); err != nil {
		return Transaction{}, err
	}
	return event, nil
}

func transactionKafkaKey(event Transaction) string {
	return event.TerminalID
}

func databasePoolConfig(dsn string) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	readPositive := func(name string, fallback int) (int, error) {
		value := os.Getenv(name)
		if value == "" {
			return fallback, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		return parsed, nil
	}
	maxConnections, err := readPositive("FRAUDWATCH_DB_MAX_CONNS", 20)
	if err != nil {
		return nil, err
	}
	minConnections, err := readPositive("FRAUDWATCH_DB_MIN_CONNS", 2)
	if err != nil {
		return nil, err
	}
	if minConnections > maxConnections {
		return nil, errors.New("FRAUDWATCH_DB_MIN_CONNS exceeds FRAUDWATCH_DB_MAX_CONNS")
	}
	config.MaxConns = int32(maxConnections)
	config.MinConns = int32(minConnections)
	config.MaxConnIdleTime = 5 * time.Minute
	return config, nil
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
	`, event.TransactionID, topic, transactionKafkaKey(event), payload)
	if err != nil {
		return false, err
	}

	return false, tx.Commit(ctx)
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" || len(requestID) > 128 {
		requestID = newRequestID()
	}
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, maxEventBytes)
	defer r.Body.Close()

	event, err := decodeTransaction(r.Body)
	if err != nil {
		s.observeRequest("invalid", time.Since(started))
		slog.Warn("transaction rejected", "request_id", requestID, "outcome", "invalid", "reason", err.Error(), "duration_ms", time.Since(started).Milliseconds())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	duplicate, err := acceptTransaction(ctx, s.db, event)
	if errors.Is(err, ErrConflict) {
		s.observeRequest("conflict", time.Since(started))
		slog.Warn("transaction rejected", "request_id", requestID, "correlation_id", event.TransactionID, "outcome", "conflict", "reason", "transaction_id_reused", "duration_ms", time.Since(started).Milliseconds())
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		s.observeRequest("unavailable", time.Since(started))
		slog.Error("transaction acceptance failed", "request_id", requestID, "correlation_id", event.TransactionID, "outcome", "unavailable", "duration_ms", time.Since(started).Milliseconds(), "error", err)
		http.Error(w, "transaction acceptance unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	outcome := "accepted"
	if duplicate {
		outcome = "duplicate"
	}
	s.observeRequest(outcome, time.Since(started))
	slog.Info("transaction accepted", "request_id", requestID, "correlation_id", event.TransactionID, "outcome", outcome, "duration_ms", time.Since(started).Milliseconds())
	_ = json.NewEncoder(w).Encode(map[string]any{
		"transaction_id": event.TransactionID,
		"status":         "accepted",
		"duplicate":      duplicate,
	})
}

func (s *Server) observeRequest(outcome string, elapsed time.Duration) {
	if s.metrics != nil {
		s.metrics.observeRequest(outcome, elapsed)
	}
}

func newRequestID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("local-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		"service", "ingestion",
		"build_revision", envOrDefault("FRAUDWATCH_BUILD_REVISION", buildRevision),
	)
	slog.SetDefault(logger)
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://fraudwatch:fraudwatch_dev@" +
			"127.0.0.1:5434/fraudwatch?sslmode=disable"
	}

	poolConfig, err := databasePoolConfig(dsn)
	if err != nil {
		slog.Error("invalid PostgreSQL pool configuration", "error", err)
		os.Exit(1)
	}
	db, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		slog.Error("PostgreSQL pool creation failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	pingCtx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	err = db.Ping(pingCtx)
	cancel()
	if err != nil {
		slog.Error("PostgreSQL unavailable", "error", err)
		os.Exit(1)
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
		slog.Error("Kafka client creation failed", "error", err)
		os.Exit(1)
	}
	defer kafka.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	metrics := newIngestionMetrics(envOrDefault("FRAUDWATCH_BUILD_REVISION", buildRevision))
	// Publication failures do not prevent HTTP acceptance.
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		runPublisher(ctx, db, kafka, metrics)
	}()

	server := &Server{db: db, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/transactions", server.ingest)
	mux.Handle("GET /metrics", metrics.handler(db))
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	readiness := func(w http.ResponseWriter, r *http.Request) {
		checkCtx, checkCancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer checkCancel()
		if err := db.Ping(checkCtx); err != nil {
			http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
	mux.HandleFunc("GET /readyz", readiness)
	mux.HandleFunc("GET /healthz", readiness)

	httpServer := &http.Server{
		Addr:              fraudwatchHTTPAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
	}

	go func() {
		slog.Info("ingestion API listening", "address", fraudwatchHTTPAddr())
		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failure", "error", err)
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

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// fraudwatchHTTPAddr preserves the loopback-only development default and lets
// the container bind to 0.0.0.0 via FRAUDWATCH_HTTP_ADDR.
func fraudwatchHTTPAddr() string {
	if addr := os.Getenv("FRAUDWATCH_HTTP_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:8080"
}
