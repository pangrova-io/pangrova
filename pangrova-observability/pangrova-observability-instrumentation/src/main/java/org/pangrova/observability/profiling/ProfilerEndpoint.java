package org.pangrova.observability.profiling;

import org.pangrova.observability.api.profiling.ProfileConfig;
import org.pangrova.observability.api.profiling.Profiler;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Controls profiling sessions: start, stop, and retrieve flame graph data.
 * Delegates to a native profiler via a functional interface to avoid module boundary violations.
 */
@Experimental
public final class ProfilerEndpoint {

    private final Profiler profilerBridge;

    public ProfilerEndpoint(Profiler profilerBridge) {
        this.profilerBridge = java.util.Objects.requireNonNull(profilerBridge, "profilerBridge");
    }

    /**
     * Starts a profiling session with the given configuration.
     *
     * @return a session ID for later retrieval
     */
    public String start(ProfileConfig config) {
        return profilerBridge.startProfiling(
                config.durationSeconds(), config.samplingFrequencyHz());
    }

    /**
     * Stops an active profiling session.
     */
    public void stop(String sessionId) {
        profilerBridge.stopProfiling(sessionId);
    }

    /**
     * Retrieves the flame graph data for a completed session.
     *
     * @return raw flame graph bytes (e.g. collapsed stack format)
     */
    public byte[] getFlameGraph(String sessionId) {
        return profilerBridge.getFlameGraphData(sessionId);
    }

}
