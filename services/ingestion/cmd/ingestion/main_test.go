package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testDSN = "postgres://user:pass@127.0.0.1:5432/database?sslmode=disable"

func TestDatabasePoolConfig(t *testing.T) {
	t.Setenv("FRAUDWATCH_DB_MAX_CONNS", "9")
	t.Setenv("FRAUDWATCH_DB_MIN_CONNS", "3")

	config, err := databasePoolConfig(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxConns != 9 || config.MinConns != 3 {
		t.Fatalf("unexpected pool size: min=%d max=%d", config.MinConns, config.MaxConns)
	}
}

func TestDatabasePoolConfigRejectsInvalidBounds(t *testing.T) {
	t.Setenv("FRAUDWATCH_DB_MAX_CONNS", "2")
	t.Setenv("FRAUDWATCH_DB_MIN_CONNS", "3")
	if _, err := databasePoolConfig(testDSN); err == nil {
		t.Fatal("expected invalid pool bounds to fail")
	}
}

func TestInvalidTransactions(t *testing.T) {
	cases := map[string]string{
		"missing ID": `{
			"customer_id":"596",
			"terminal_id":"3156",
			"amount":57.16,
			"occurred_at":"2018-04-01T00:00:31Z"
		}`,
		// "zero amount": `{
		// 	"transaction_id":"x",
		// 	"customer_id":"596",
		// 	"terminal_id":"3156",
		// 	"amount":0,
		// 	"occurred_at":"2018-04-01T00:00:31Z"
		// }`,
		"bad timestamp": `{
			"transaction_id":"x",
			"customer_id":"596",
			"terminal_id":"3156",
			"amount":57.16,
			"occurred_at":"invalid"
		}`,
		"unknown field": `{
			"transaction_id":"x",
			"customer_id":"596",
			"terminal_id":"3156",
			"amount":57.16,
			"occurred_at":"2018-04-01T00:00:31Z",
			"unexpected":true
		}`,
	}

	server := &Server{}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/transactions",
				strings.NewReader(body),
			)
			resp := httptest.NewRecorder()
			server.ingest(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("got %d, expected 400", resp.Code)
			}
		})
	}
}
