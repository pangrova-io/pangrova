package org.pangrova.spi.connector.metadata;

/** Returned only after the format authority commits an exact irrevocable intent abort barrier. */
public record DistributedWriteAbortReceipt(String executionId, String commitFingerprint) {
    public DistributedWriteAbortReceipt {
        if (executionId == null || executionId.isBlank() || commitFingerprint == null
                || !commitFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Durable abort receipt requires exact intent identity");
        }
    }
}
