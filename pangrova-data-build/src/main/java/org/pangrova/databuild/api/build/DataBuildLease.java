package org.pangrova.databuild.api.build;

import org.pangrova.databuild.api.DataBuildPageKind;

import java.time.Instant;
import java.util.List;

/** Fenced lease granted to one Coordinator for dispatching a work page. */
public record DataBuildLease(
        String tenantId,
        String jobId,
        String pageId,
        int stageId,
        int partitionId,
        int pageOrdinal,
        DataBuildPageKind kind,
        List<DataBuildSplit> splits,
        List<String> dependencyPageIds,
        int attemptId,
        long fencingToken,
        String leaseOwner,
        long ownerEpoch,
        Instant expiresAt,
        org.pangrova.query.api.coordination.ExecutionAnchor executionAnchor) {

    public DataBuildLease {
        requireIdentity(tenantId, jobId, pageId, leaseOwner);
        requireCoordinates(stageId, partitionId, pageOrdinal, attemptId);
        requireFence(fencingToken, ownerEpoch, expiresAt);
        requireKind(kind);
        splits = List.copyOf(splits);
        dependencyPageIds = List.copyOf(dependencyPageIds);
        requireInputs(kind, splits, dependencyPageIds);
        java.util.Objects.requireNonNull(executionAnchor, "executionAnchor");
        if (!new DataBuildJobKey(tenantId, jobId).executionId().equals(executionAnchor.executionId())
                || !leaseOwner.equals(executionAnchor.ownerId())) {
            throw new IllegalArgumentException("artifact lease differs from its real execution anchor");
        }
    }

    private static void requireIdentity(
            String tenantId, String jobId, String pageId, String leaseOwner) {
        if (tenantId == null || tenantId.isBlank()
                || jobId == null || jobId.isBlank() || pageId == null || pageId.isBlank()
                || leaseOwner == null || leaseOwner.isBlank()) {
            throw new IllegalArgumentException("lease identities must not be blank");
        }
    }

    private static void requireCoordinates(
            int stageId, int partitionId, int pageOrdinal, int attemptId) {
        if (stageId < 0 || partitionId < 0 || pageOrdinal < 0 || attemptId < 0) {
            throw new IllegalArgumentException("invalid artifact work-page lease");
        }
    }

    private static void requireFence(long fencingToken, long ownerEpoch, Instant expiresAt) {
        if (fencingToken <= 0 || ownerEpoch <= 0 || expiresAt == null) {
            throw new IllegalArgumentException("invalid artifact work-page lease");
        }
    }

    private static void requireKind(DataBuildPageKind kind) {
        if (kind == null) {
            throw new IllegalArgumentException("lease kind must not be null");
        }
    }

    private static void requireInputs(
            DataBuildPageKind kind,
            List<DataBuildSplit> splits,
            List<String> dependencyPageIds) {
        if (kind == DataBuildPageKind.RELATIONAL_COMPUTE
                || kind == DataBuildPageKind.RELATIONAL_PARTITION) {
            if (!splits.isEmpty() && !dependencyPageIds.isEmpty()) {
                throw new IllegalArgumentException("relational exchange lease cannot also enumerate source splits");
            }
            return;
        }
        boolean sourcePage = kind == DataBuildPageKind.SOURCE_MAP;
        if (sourcePage != (splits.size() == 1 && dependencyPageIds.isEmpty())
                || (!sourcePage && (!splits.isEmpty() || dependencyPageIds.isEmpty()))) {
            throw new IllegalArgumentException("artifact lease inputs differ from its kind");
        }
    }

    public List<String> sourceObjectIds() {
        return splits.stream().map(DataBuildSplit::dataObjectId).toList();
    }
}
