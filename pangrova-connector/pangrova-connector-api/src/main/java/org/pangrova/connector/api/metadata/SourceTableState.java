package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Format-owned metadata bytes and the scalars checked by the authoritative catalog CAS. */
@Experimental
public record SourceTableState(SourceTableKey key, String tableId, String datasetUri, long generation, long revision,
        long version, long rows, byte[] schemaSha256, byte[] payload) {
    public SourceTableState {
        java.util.Objects.requireNonNull(key);
        text(tableId);
        text(datasetUri);
        if (generation <= 0 || revision <= 0 || version < 0 || rows < 0
                || schemaSha256.length != 32 || payload.length == 0 || payload.length > 4 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid source table state");
        }
        schemaSha256 = schemaSha256.clone();
        payload = payload.clone();
    }
    @Override public byte[] schemaSha256() { return schemaSha256.clone(); }
    @Override public byte[] payload() { return payload.clone(); }
    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }
}
