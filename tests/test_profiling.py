from fraudwatch_ml.profiling import EventTimings, RollingProfiler


def test_rolling_profiler_percentiles_and_window():
    profiler = RollingProfiler(window=3)
    for milliseconds in (1, 2, 3, 4):
        timings = EventTimings()
        timings.add("history_lookup", milliseconds * 1_000_000)
        profiler.observe(timings)

    count, mean, p50, p95 = profiler.summary("history_lookup")
    assert count == 3
    assert mean == 3
    assert p50 == 3
    assert abs(p95 - 3.9) < 1e-9
    assert profiler.summary("unknown") is None


def test_event_timings_accumulates():
    timings = EventTimings()
    timings.add("parse", 150_000)
    timings.add("parse", 250_000)
    assert timings.durations_ns["parse"] == 400_000
