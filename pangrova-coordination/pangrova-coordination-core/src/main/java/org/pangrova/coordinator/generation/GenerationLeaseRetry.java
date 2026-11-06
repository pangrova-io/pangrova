package org.pangrova.coordinator.generation;

import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.generation.api.execution.GenerationExecutionOwnerLostException;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Separates an authenticated execution exit from permission to requeue durable publication work. */
final class GenerationLeaseRetry {
    private static final Logger LOG = LoggerFactory.getLogger(GenerationLeaseRetry.class);

    private GenerationLeaseRetry() {}

    static void retry(GenerationJobStore store, GenerationLease lease,
                      Instant nextAvailableAt, String reason, Instant now) {
        if (jobTerminated(store, lease)) return;
        try {
            store.retryPage(lease, nextAvailableAt, reason, now);
        } catch (GenerationExecutionOwnerLostException staleOwner) {
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

    private static boolean jobTerminated(GenerationJobStore store, GenerationLease lease) {
        var job = store.findJob(lease.tenantId(), lease.jobId()).orElseThrow(() ->
                new IllegalStateException(
                        "Exact artifact job disappeared before its attempt retired: " + lease.jobId()));
        return switch (job.state()) {
            case CANCELLED, FAILED, PUBLISHED -> true;
            case QUEUED, RUNNING, READY_TO_PUBLISH -> false;
        };
    }
}
