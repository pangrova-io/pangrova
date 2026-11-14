package org.pangrova.observability.api.profiling;

import org.pangrova.common.api.annotation.Experimental;

/** Profiling provider. Unsupported operations must throw ProfilingUnavailableException, never
 * fabricate a session or empty successful result. The process assembly owns the provider.
 */
@Experimental
public interface Profiler {
    String startProfiling(int durationSeconds, int samplingFrequencyHz);
    void stopProfiling(String sessionId);
    byte[] getFlameGraphData(String sessionId);
}
