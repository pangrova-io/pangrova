package org.pangrova.persistence.api.query.lineage;

import org.pangrova.spi.query.lineage.QueryLineageFact;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.pangrova.common.api.annotation.Stable;

/** Durable at-least-once delivery; HTTP acceptance and physical query cleanup are different facts. */
@Stable(since = "1.0")
public interface QueryLineageOutbox {
    /** Commits the frozen fact or rejects conflicting reuse of its logical identity. */
    void append(QueryLineageFact fact);
    /** Claims only this deployment namespace; a terminal cannot precede its acknowledged START. */
    Optional<Delivery> claim(String namespace, Duration lease);
    /** Acknowledges an actual accepted HTTP send only while this exact lease remains current. */
    void acknowledge(Delivery delivery);
    /** Makes an unsuccessful send retryable without deleting its durable fact. */
    void release(Delivery delivery);
    /** Counts durable unacknowledged facts, including currently entered physical sends. */
    long pendingCount();

    /** A frozen logical fact and its actual monotonically issued database delivery owner. */
    record Delivery(QueryLineageFact fact, long token) {
        /** Rejects invented or absent lease ownership before delivery acknowledgement. */
        public Delivery {
            Objects.requireNonNull(fact, "fact");
            if (token <= 0) throw new IllegalArgumentException("Delivery token must be positive");
        }
    }
}
