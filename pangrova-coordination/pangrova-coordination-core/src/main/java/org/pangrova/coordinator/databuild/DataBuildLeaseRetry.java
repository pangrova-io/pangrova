package org.pangrova.coordinator.databuild;

import org.pangrova.databuild.api.build.DataBuildLease;
import org.pangrova.databuild.api.build.DataBuildStore;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwnerLostException;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Separates an authenticated execution exit from permission to requeue durable publication work. */
final class DataBuildLeaseRetry {
    private static final Logger LOG = LoggerFactory.getLogger(DataBuildLeaseRetry.class);

    private DataBuildLeaseRetry() {}

    static void retry(DataBuildStore store, DataBuildLease lease,
                      Instant nextAvailableAt, String reason, Instant now) {
        if (jobTerminated(store, lease)) return;
        try {
            store.retryPage(lease, nextAvailableAt, reason, now);
        } catch (DataBuildExecutionOwnerLostException staleOwner) {
            // A new publication owner is authoritative. Its work is not
            // changed merely because this exact old Native attempt exited.
            LOG.warn("event=artifact_retry_rejected_owner_lost jobId={} pageId={}", lease.jobId(), lease.pageId());
        } catch (AuthorizationDeniedException revoked) {
            // Revocation can commit between the read and the same-transaction
            // authorization gate. Only real durable termination closes it;
            // a live job with unavailable/revoked authority remains retryable.
            if (!jobTerminated(store, lease)) throw revoked;
        }
    }

    private static boolean jobTerminated(DataBuildStore store, DataBuildLease lease) {
        var job = store.findJob(lease.tenantId(), lease.jobId()).orElseThrow(() ->
                new IllegalStateException(
                        "Exact artifact job disappeared before its attempt retired: " + lease.jobId()));
        return switch (job.state()) {
            case CANCELLED, FAILED, PUBLISHED -> true;
            case QUEUED, RUNNING, READY_TO_PUBLISH -> false;
        };
    }
}
