"""Shared checked timing summaries for RustSpark's local SQL benchmark harnesses."""

import math
import statistics


def summarize_samples(samples):
    """Return rounded samples, median and nearest-rank p95 in milliseconds."""
    if not samples:
        raise ValueError("Benchmark must have at least one timing sample")
    checked = []
    for duration in samples:
        if (isinstance(duration, bool) or not isinstance(duration, (int, float))
                or not math.isfinite(duration) or duration < 0):
            raise ValueError(f"Invalid benchmark duration: {duration}")
        checked.append(round(float(duration), 3))
    ordered = sorted(checked)
    return {
        "samples_ms": checked,
        "median_ms": round(statistics.median(checked), 3),
        "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
    }
