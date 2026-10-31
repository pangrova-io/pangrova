package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.profiling.ProfilerEndpoint;

/**
 * No-op profiler bridge for nodes without native engine (e.g. Coordinator).
 * Returns empty data; endpoint becomes functional when Worker-side profiling is wired.
 */
public final class DisabledNativeProfiler implements ProfilerEndpoint.NativeProfilerBridge {

    @Override
    public String startProfiling(int durationSeconds, int samplingFrequencyHz) {
        return "noop-session";
    }

    @Override
    public void stopProfiling(String sessionId) {
        // no-op: node has no native engine
    }

    @Override
    public byte[] getFlameGraphData(String sessionId) {
        return new byte[0];
    }
}
