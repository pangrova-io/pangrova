package org.pangrova.protocol.rest.maintenance;

import java.util.UUID;

/** Authenticated request to stage a bounded rewrite against an exact connector revision. */
public record ReclusterStageRequest(String catalogBinding, String tenant, String principal, String schema,
        String table, String revision, UUID runId, UUID attemptId, long deadlineEpochMs) {

    public static final String PATH = "/v1/maintenance/recluster-stage";
    public static final int MAX_REQUEST_BYTES = 16 * 1024;
    public static final int MAX_RESULT_BYTES = 8 * 1024 * 1024;
    public static final long TIMEOUT_MS = 120_000;
}
