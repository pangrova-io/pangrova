package org.pangrova.observability.noop;

import org.pangrova.observability.api.profiling.ProfilingUnavailableException;
import org.pangrova.observability.spi.profiling.Profiler;

/** Explicit node configuration with no profiling implementation. Never reports fabricated success. */
public final class UnavailableProfiler implements Profiler {
    @Override public String startProfiling(int durationSeconds, int samplingFrequencyHz) { throw unavailable(); }
    @Override public void stopProfiling(String sessionId) { throw unavailable(); }
    @Override public byte[] getFlameGraphData(String sessionId) { throw unavailable(); }
    private static ProfilingUnavailableException unavailable() {
        return new ProfilingUnavailableException("Profiling is not supported on this node");
    }
}
