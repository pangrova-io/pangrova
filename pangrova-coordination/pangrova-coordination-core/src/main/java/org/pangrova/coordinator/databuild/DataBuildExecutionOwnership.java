package org.pangrova.coordinator.databuild;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import java.util.Objects;
import java.util.Optional;

/** Artifact routing through the same durable HA ownership registry used by queries. */
public final class DataBuildExecutionOwnership {
    private final HighAvailabilityCoordinationService coordination;
    private final String coordinatorId;
    private final org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority processAuthority;
    private final java.util.concurrent.ConcurrentHashMap<String, ExecutionAnchor> publicationAnchors =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Binds the actual started Coordinator process, not a generated maintenance principal. */
    public DataBuildExecutionOwnership(HighAvailabilityCoordinationService coordination, String coordinatorId) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        if (coordinatorId == null || coordinatorId.isBlank()) {
            throw new IllegalArgumentException("coordinatorId must not be blank");
        }
        this.coordinatorId = coordinatorId;
        this.processAuthority = null;
    }

    /** Maintenance-only publication shares the bootstrap's real backend and process lifecycle. */
    public DataBuildExecutionOwnership(org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority authority) {
        processAuthority = Objects.requireNonNull(authority, "authority");
        coordinatorId = authority.coordinatorId();
        coordination = null;
    }

    /** Acquires or recovers the job's backend-issued anchor; another live owner is never overridden. */
    public Optional<ExecutionAnchor> acquire(DataBuildJobKey job) {
        if (processAuthority != null) {
            var acquired = processAuthority.acquire(job.executionId());
            acquired.ifPresent(anchor -> publicationAnchors.put(job.executionId(), anchor));
            return acquired;
        }
        if (!coordination.acquireQueryOwnership(job.executionId(), coordinatorId)) {
            return Optional.empty();
        }
        return Optional.of(coordination.requireExecutionAnchor(job.executionId()));
    }

    /** Yields completed computation to a publisher or a replacement process without inventing a token. */
    public void yield(ExecutionAnchor anchor) {
        if (processAuthority != null) {
            processAuthority.yield(anchor);
            publicationAnchors.remove(anchor.executionId(), anchor);
            return;
        }
        coordination.yieldExecution(anchor);
    }

    /** Releases only this process's exact durable execution identity. */
    public void release(DataBuildJobKey job) {
        if (processAuthority != null) {
            var anchor = publicationAnchors.get(job.executionId());
            if (anchor != null) {
                processAuthority.release(anchor);
                publicationAnchors.remove(job.executionId(), anchor);
            }
            return;
        }
        coordination.releaseQueryOwnership(job.executionId(), coordinatorId);
    }

    /** Returns the actual owning Coordinator identifier. */
    public String coordinatorId() {
        return coordinatorId;
    }
}
