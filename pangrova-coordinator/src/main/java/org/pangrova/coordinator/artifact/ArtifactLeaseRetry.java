package org.pangrova.coordinator.artifact;

import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.ArtifactExecutionFence;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Separates an authenticated execution exit from permission to requeue durable publication work. */
final class ArtifactLeaseRetry {
    private static final Logger LOG = LoggerFactory.getLogger(ArtifactLeaseRetry.class);

    private ArtifactLeaseRetry() {}

    static void retry(ArtifactBuildStore store, ArtifactBuildLease lease,
                      Instant nextAvailableAt, String reason, Instant now) {
        if (jobTerminated(store, lease)) return;
        try {
            store.retryPage(lease, nextAvailableAt, reason, now);
        } catch (ArtifactExecutionFence.LostOwnerException staleOwner) {
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

    private static boolean jobTerminated(ArtifactBuildStore store, ArtifactBuildLease lease) {
        var job = store.findJob(lease.tenantId(), lease.jobId()).orElseThrow(() ->
                new IllegalStateException(
                        "Exact artifact job disappeared before its attempt retired: " + lease.jobId()));
        return switch (job.state()) {
            case CANCELLED, FAILED, PUBLISHED -> true;
            case QUEUED, RUNNING, READY_TO_PUBLISH -> false;
        };
    }
}
