"""Shared checked timing summaries for RustSpark's local SQL benchmark harnesses."""

import math


def _validated_samples(samples):
    """Validate durations once before computing timing statistics."""
    if not samples:
        raise ValueError("Benchmark must have at least one timing sample")
    measured = []
    for duration in samples:
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration < 0:
            raise ValueError(f"Invalid benchmark duration: {duration}")
        try:
            value = float(duration)
        except OverflowError as error:
            raise ValueError(f"Invalid benchmark duration: {duration}") from error
        if not math.isfinite(value):
            raise ValueError(f"Invalid benchmark duration: {duration}")
        measured.append(value)
    if not measured:
        raise ValueError("Benchmark must have at least one timing sample")
    return measured


def summarize_samples(samples):
    """Return rounded samples, median and nearest-rank p95 in milliseconds."""
    measured = _validated_samples(samples)
    ordered = sorted(measured)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (
        ordered[middle - 1] + (ordered[middle] - ordered[middle - 1]) / 2
    )
    return {
        "samples_ms": [round(value, 3) for value in measured],
        "median_ms": round(median, 3),
        "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "p99_ms": round(ordered[math.ceil(len(ordered) * 0.99) - 1], 3),
    }
