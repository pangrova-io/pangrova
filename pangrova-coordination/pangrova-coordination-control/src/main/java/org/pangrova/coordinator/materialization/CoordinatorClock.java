package org.pangrova.coordinator.materialization;

import java.time.Instant;
import org.pangrova.materialization.api.port.Clock;

/** System clock adapter for the materialization kernel. */
public final class CoordinatorClock implements Clock {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
