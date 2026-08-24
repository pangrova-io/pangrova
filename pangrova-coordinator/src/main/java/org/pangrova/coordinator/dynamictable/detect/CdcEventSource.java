package org.pangrova.coordinator.dynamictable.detect;

import java.time.Duration;
import java.util.List;

import org.pangrova.spi.streaming.CdcEnvelope;

/**
 * Abstracts CDC event consumption for change detection.
 *
 * <p>Unlike {@code CdcConsumer} which follows a write-then-commit-offset
 * pattern, this interface provides read-only access to CDC events.
 * The {@link CdcChangeDetector} consumes events and returns them as
 * {@link ChangeResult#cdcBatch} without writing to a target table.</p>
 */
public interface CdcEventSource {

    /**
     * Poll a batch of CDC events starting from the given offset.
     *
     * @param offset  opaque CDC offset to resume from (nullable — start from earliest)
     * @param timeout maximum time to wait for events
     * @return list of CDC envelopes (empty if no events within timeout)
     */
    List<CdcEnvelope> poll(String offset, Duration timeout);
}
