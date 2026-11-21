package org.pangrova.connector.api.metadata;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

/** Returned only after the format authority commits an exact irrevocable intent abort barrier. */
@Experimental
public record DistributedWriteAbortReceipt(String executionId, String commitFingerprint) {
    public DistributedWriteAbortReceipt {
        if (executionId == null || executionId.isBlank() || commitFingerprint == null
                || !DigestUtils.isSha256Hex(commitFingerprint)) {
            throw new IllegalArgumentException("Durable abort receipt requires exact intent identity");
        }
    }
}
