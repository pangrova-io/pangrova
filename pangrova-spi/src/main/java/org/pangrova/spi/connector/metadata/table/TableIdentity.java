package org.pangrova.spi.connector.metadata.table;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Format-level identity of a physical table.
 *
 * <p>This is the evidence a controlled operation uses to prove it is acting on the
 * table that was admitted, not on a different table that happens to have the same
 * catalog name. Iceberg exposes a durable table UUID; Paimon may synthesize its
 * UUID from the table name, so {@code location} and {@code schemaFingerprint} carry
 * the remaining evidence.</p>
 *
 * <p>Connectors that cannot establish a format identity return
 * {@link java.util.Optional#empty()} from the metadata SPI rather than inventing
 * one. A missing identity degrades fencing to name matching and must never be
 * treated as a matching identity.</p>
 */
@Experimental
public record TableIdentity(
        String format,
        String formatTableUuid,
        String location,
        String schemaFingerprint) {

    public TableIdentity {
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("format must not be blank");
        }
        formatTableUuid = formatTableUuid == null ? "" : formatTableUuid;
        location = location == null ? "" : location;
        schemaFingerprint = schemaFingerprint == null ? "" : schemaFingerprint;
    }

    /** Returns true when the format publishes a durable table UUID. */
    public boolean hasFormatUuid() {
        return !formatTableUuid.isBlank();
    }
}
