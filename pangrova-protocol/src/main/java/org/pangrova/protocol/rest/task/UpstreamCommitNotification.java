package org.pangrova.protocol.rest.task;

import java.util.Objects;

/**
 * Notification sent from Coordinator to Worker when an upstream stage
 * has committed its output. Enables BLOCKING exchange sources to begin
 * delivering data to downstream tasks.
 *
 * @param queryId          the query identifier
 * @param upstreamStageId  the upstream stage that committed
 */
public record UpstreamCommitNotification(
        String queryId,
        int upstreamStageId
) {
    public UpstreamCommitNotification {
        Objects.requireNonNull(queryId, "queryId must not be null");
    }
}
