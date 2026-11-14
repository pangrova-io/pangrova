package org.pangrova.streaming.api.watermark;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.Optional;

/** Per-source monotonic event-time position. Retention across restarts depends on the selected backend. */
@Experimental
public interface WatermarkStore {
    /**
     * Atomically stores the later of the proposed and recorded positions and returns the stored position.
     * Concurrent or repeated updates cannot regress a source. Failed writes propagate to the caller.
     */
    Instant advance(String tableName, Instant watermark);

    /** Returns the recorded position, or empty when this source has no position. */
    Optional<Instant> load(String tableName);
}
