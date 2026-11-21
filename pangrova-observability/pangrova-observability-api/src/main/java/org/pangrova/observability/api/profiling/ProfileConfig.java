package org.pangrova.observability.api.profiling;

/** Positive duration and sampling frequency for a profiling session. */
public record ProfileConfig(int durationSeconds, int samplingFrequencyHz) {
    public ProfileConfig {
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("durationSeconds must be positive");
        }
        if (samplingFrequencyHz <= 0) {
            throw new IllegalArgumentException("samplingFrequencyHz must be positive");
        }
    }
}
