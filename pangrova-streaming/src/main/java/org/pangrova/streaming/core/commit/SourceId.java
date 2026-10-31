package org.pangrova.streaming.core.commit;

import java.util.Objects;

/**
 * Identifies a CDC source (e.g., a Kafka topic or Debezium connector instance).
 *
 * <p>Used as part of the (source_id, partition, offset) deduplication triple
 * in the CDC exactly-once semantics protocol.</p>
 *
 * @param value the source identifier string (must not be null or blank)
 */
public record SourceId(String value) {

    public SourceId {
        Objects.requireNonNull(value, "source id value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("source id value must not be blank");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
