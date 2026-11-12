package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type transactionContractCorpus struct {
	Cases []struct {
		Name                 string `json:"name"`
		Payload              string `json:"payload"`
		KafkaKey             string `json:"kafka_key"`
		ValidPayload         bool   `json:"valid_payload"`
		ValidStream          bool   `json:"valid_stream"`
		NormalizedOccurredAt string `json:"normalized_occurred_at"`
	} `json:"cases"`
}

func TestTransactionContractFixtures(t *testing.T) {
	path := filepath.Join(
		"..", "..", "..", "..", "contracts", "fixtures", "transaction.v1.json",
	)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var corpus transactionContractCorpus
	if err := json.Unmarshal(content, &corpus); err != nil {
		t.Fatal(err)
	}

	for _, fixture := range corpus.Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			event, err := decodeTransaction(strings.NewReader(fixture.Payload))
			if fixture.ValidPayload != (err == nil) {
				t.Fatalf("payload validity=%v, error=%v", fixture.ValidPayload, err)
			}
			if err != nil {
				return
			}

			if event.OccurredAt != fixture.NormalizedOccurredAt {
				t.Fatalf(
					"normalized timestamp=%q, want %q",
					event.OccurredAt,
					fixture.NormalizedOccurredAt,
				)
			}
			streamValid := transactionKafkaKey(event) == fixture.KafkaKey
			if streamValid != fixture.ValidStream {
				t.Fatalf("stream validity=%v, want %v", streamValid, fixture.ValidStream)
			}
		})
	}
}

func TestTransactionHTTPPayloadLimit(t *testing.T) {
	body := `{"transaction_id":"tx-large","customer_id":"` +
		strings.Repeat("x", maxEventBytes) +
		`","terminal_id":"terminal-1","amount":1,` +
		`"occurred_at":"2018-04-01T00:00:00Z"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/transactions",
		strings.NewReader(body),
	)
	response := httptest.NewRecorder()
	(&Server{}).ingest(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", response.Code)
	}
}
