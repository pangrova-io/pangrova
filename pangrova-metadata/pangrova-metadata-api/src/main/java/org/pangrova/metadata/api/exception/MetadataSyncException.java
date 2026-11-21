package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when metadata synchronization fails after all retry attempts are exhausted.
 * Preserves the original exception chain so upstream callers can distinguish
 * failure reasons (network timeout, authentication failure, metadata inconsistency, etc.).
 */
public class MetadataSyncException extends MetadataException {

    private final String catalogName;
    private final int attemptsExhausted;

    /** Records the exhausted retry count and retains the last underlying failure. */
    public MetadataSyncException(String catalogName, int attemptsExhausted, Throwable cause) {
        super(PangrovaErrorCode.METADATA_SYNC_FAILED,
                String.format("Metadata sync failed for catalog=%s after %d attempts", catalogName, attemptsExhausted),
                cause);
        this.catalogName = catalogName;
        this.attemptsExhausted = attemptsExhausted;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public int getAttemptsExhausted() {
        return attemptsExhausted;
    }
}
