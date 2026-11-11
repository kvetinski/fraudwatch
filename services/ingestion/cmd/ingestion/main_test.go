package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
