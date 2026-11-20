package main

import (
	"math"
	"testing"
	"time"
)

func TestLocalHistogramUsesCumulativeBuckets(t *testing.T) {
	histogram := newLocalHistogram([]float64{0.1, 1})
	histogram.observe(0.05)
	histogram.observe(0.5)
	histogram.observe(2)

	if histogram.count != 3 || math.Abs(histogram.sum-2.55) > 1e-12 {
		t.Fatalf("unexpected histogram totals: count=%d sum=%g", histogram.count, histogram.sum)
	}
	if histogram.counts[0] != 1 || histogram.counts[1] != 2 {
		t.Fatalf("buckets are not cumulative: %v", histogram.counts)
	}
}

func TestMetricsInitializeStableOutcomeSeries(t *testing.T) {
	metrics := newIngestionMetrics("test")
	for _, outcome := range []string{"accepted", "duplicate", "invalid", "conflict", "unavailable"} {
		if value, exists := metrics.requests[outcome]; !exists || value != 0 {
			t.Fatalf("request outcome %q missing its zero series", outcome)
		}
	}
	for _, result := range []string{"success", "failure", "empty"} {
		if value, exists := metrics.outboxAttempts[result]; !exists || value != 0 {
			t.Fatalf("outbox result %q missing its zero series", result)
		}
	}

	metrics.observeRequest("accepted", 25*time.Millisecond)
	metrics.observeOutbox("success", 3, 50*time.Millisecond)
	if metrics.requests["accepted"] != 1 || metrics.outboxAttempts["success"] != 1 {
		t.Fatal("observations did not increment counters")
	}
	if metrics.outboxPublished != 3 || metrics.outboxLastSuccess == 0 {
		t.Fatal("successful outbox publication was not recorded")
	}
}

func TestPrometheusLabelEscaping(t *testing.T) {
	got := escapePrometheusLabel("a\\b\"c\nd")
	want := `a\\b\"c\nd`
	if got != want {
		t.Fatalf("escaped value %q, want %q", got, want)
	}
}
