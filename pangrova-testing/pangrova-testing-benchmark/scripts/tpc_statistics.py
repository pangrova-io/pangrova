"""Shared TPC workload latency statistics. R7 is explicit; qualification is policy."""

import math


PERCENTILE_METHOD = "R7"
MINIMUM_SAMPLES = 1000
TIMING_SCOPE = "cli-end-to-end"


def percentile(ordered, probability):
    """Hyndman-Fan R7: zero-based rank p*(n-1), linearly interpolated."""
    rank = probability * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def sample_statistics(samples, minimum_samples=MINIMUM_SAMPLES):
    if type(minimum_samples) is not int or minimum_samples < MINIMUM_SAMPLES:
        raise ValueError(f"minimumSamples must be an integer >= {MINIMUM_SAMPLES}")
    if not isinstance(samples, list) or not samples:
        raise ValueError("samples must be a non-empty array")
    for value in samples:
        if type(value) not in (int, float) or value <= 0:
            raise ValueError("samples must contain positive finite JSON numbers")
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError("samples must contain positive finite JSON numbers")
    ordered = sorted(samples)
    qualified = len(samples) >= minimum_samples
    try:
        minimum = ordered[0]
        mean = minimum + math.fsum((value - minimum) / len(samples) for value in samples)
    except OverflowError as error:
        raise ValueError("samples produce a non-finite mean") from error
    result = {
        "sampleCount": len(samples), "minimumSamples": minimum_samples,
        "percentileQualified": qualified,
        "p50Ms": percentile(ordered, 0.50), "minMs": ordered[0], "maxMs": ordered[-1],
        "meanMs": mean,
    }
    if qualified:
        result["p99Ms"] = percentile(ordered, 0.99)
    return result
