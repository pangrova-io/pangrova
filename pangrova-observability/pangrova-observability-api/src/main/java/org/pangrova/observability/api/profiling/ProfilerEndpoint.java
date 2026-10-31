package org.pangrova.observability.api.profiling;

/**
 * Controls profiling sessions: start, stop, and retrieve flame graph data.
 * Delegates to a native profiler via a functional interface to avoid module boundary violations.
 */
public final class ProfilerEndpoint {

    private final NativeProfilerBridge profilerBridge;

    public ProfilerEndpoint(NativeProfilerBridge profilerBridge) {
        this.profilerBridge = profilerBridge;
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

    /**
     * Profiling session configuration.
     */
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

    /**
     * Functional bridge to native profiler. Avoids direct dependency on native module.
     */
    public interface NativeProfilerBridge {
        String startProfiling(int durationSeconds, int samplingFrequencyHz);
        void stopProfiling(String sessionId);
        byte[] getFlameGraphData(String sessionId);
    }
}
