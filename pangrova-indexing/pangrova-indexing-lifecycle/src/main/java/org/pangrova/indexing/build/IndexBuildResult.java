package org.pangrova.indexing.build;

import org.pangrova.indexing.enums.IndexBuildOutcome;

import java.util.Objects;

public final class IndexBuildResult {
    private final int successCount;
    private final int failureCount;
    private final IndexBuildOutcome outcome;

    public IndexBuildResult(int successCount, int failureCount) {
        this(successCount, failureCount, IndexBuildOutcome.COMPLETED);
    }

    private IndexBuildResult(int successCount, int failureCount, IndexBuildOutcome outcome) {
        if (successCount < 0) {
            throw new IllegalArgumentException("successCount must not be negative, got " + successCount);
        }
        if (failureCount < 0) {
            throw new IllegalArgumentException("failureCount must not be negative, got " + failureCount);
        }
        this.successCount = successCount;
        this.failureCount = failureCount;
        this.outcome = outcome;
    }

    public static IndexBuildResult success(int successCount) {
        return new IndexBuildResult(successCount, 0, IndexBuildOutcome.COMPLETED);
    }

    public static IndexBuildResult claimFailed() {
        return new IndexBuildResult(0, 0, IndexBuildOutcome.CLAIM_FAILED);
    }

    public static IndexBuildResult staleGeneration() {
        return new IndexBuildResult(0, 0, IndexBuildOutcome.STALE_GENERATION);
    }

    /** A cancelled attempt must never authorize generation publication. */
    public static IndexBuildResult cancelled() {
        return new IndexBuildResult(0, 0, IndexBuildOutcome.CANCELLED);
    }

    public static IndexBuildResult rejected() {
        return new IndexBuildResult(0, 0, IndexBuildOutcome.REJECTED);
    }

    public int successCount() {
        return successCount;
    }

    public int failureCount() {
        return failureCount;
    }

    public IndexBuildOutcome outcome() {
        return outcome;
    }

    public boolean isFullySuccessful() {
        return outcome == IndexBuildOutcome.COMPLETED && failureCount == 0;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IndexBuildResult that)) {
            return false;
        }
        return successCount == that.successCount
                && failureCount == that.failureCount
                && outcome == that.outcome;
    }

    @Override
    public int hashCode() {
        return Objects.hash(successCount, failureCount, outcome);
    }

    @Override
    public String toString() {
        return "IndexBuildResult{outcome=" + outcome + ", successCount=" + successCount
                + ", failureCount=" + failureCount + "}";
    }

}
