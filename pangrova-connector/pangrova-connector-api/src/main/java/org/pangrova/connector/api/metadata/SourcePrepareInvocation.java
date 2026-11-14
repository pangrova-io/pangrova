package org.pangrova.connector.api.metadata;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

/** Durable issuance identity for one SDK preparation call; expiry is never drain evidence. */
@Experimental
public record SourcePrepareInvocation(String executionId, String fingerprint, String invocationId,
        int pageIndex, String afterPath, String workerEndpoint, byte[] requestSha256) {
    private static final int MAX_WORKER_ENDPOINT_LENGTH = 2048;

    public SourcePrepareInvocation {
        requireExecutionIdentity(executionId, fingerprint, invocationId);
        if (pageIndex < 0
                || pageIndex >= SourceObjectPage.MAX_PAGES || afterPath == null
                || (pageIndex == 0) != afterPath.isEmpty()
                || workerEndpoint == null || workerEndpoint.isBlank()
                || workerEndpoint.length() > MAX_WORKER_ENDPOINT_LENGTH
                || requestSha256 == null || requestSha256.length != 32) {
            throw new IllegalArgumentException("Incomplete preparation issuance identity");
        }
        requestSha256 = requestSha256.clone();
    }
    @Override public byte[] requestSha256() { return requestSha256.clone(); }

    private static void requireExecutionIdentity(String executionId, String fingerprint, String invocationId) {
        if (executionId == null || executionId.isBlank() || fingerprint == null
                || !DigestUtils.isSha256Hex(fingerprint) || invocationId == null
                || !invocationId.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("Incomplete preparation issuance identity");
        }
    }
}
