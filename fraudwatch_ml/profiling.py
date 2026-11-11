"""Small bounded-window latency profiler for local FraudWatch benchmarks.

Use rolling observations, not an unbounded list, to keep profiling memory stable.
Time measurements are per message in milliseconds; p95 is interpolated.
"""
from collections import defaultdict, deque
from dataclasses import dataclass, field
from math import floor


@dataclass
class EventTimings:
    durations_ns: dict[str, int] = field(default_factory=dict)

    def add(self, stage: str, duration_ns: int) -> None:
        if duration_ns < 0:
            raise ValueError("duration_ns must be nonnegative")
        self.durations_ns[stage] = self.durations_ns.get(stage, 0) + duration_ns


class RollingProfiler:
    def __init__(self, window: int = 10_000):
        if window < 1:
            raise ValueError("window must be positive")
        self._samples: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=window)
        )

    def observe(self, timing: EventTimings) -> None:
        for stage, duration_ns in timing.durations_ns.items():
            self._samples[stage].append(duration_ns / 1_000_000)

    @staticmethod
    def _percentile(sorted_values: list[float], fraction: float) -> float:
        index = (len(sorted_values) - 1) * fraction
        low = floor(index)
        high = min(low + 1, len(sorted_values) - 1)
        return sorted_values[low] + (index - low) * (
            sorted_values[high] - sorted_values[low]
        )

    def summary(self, stage: str) -> tuple[int, float, float, float] | None:
        values = self._samples.get(stage)
        if not values:
            return None
        ordered = sorted(values)
        return (
            len(ordered),
            sum(ordered) / len(ordered),
            self._percentile(ordered, 0.5),
            self._percentile(ordered, 0.95),
        )
