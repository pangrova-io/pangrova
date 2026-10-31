package org.pangrova.indexing.core.build;

import java.util.Objects;

public final class IndexBuildResult {
    private final int successCount;
    private final int failureCount;
    private final Outcome outcome;

    public IndexBuildResult(int successCount, int failureCount) {
        this(successCount, failureCount, Outcome.COMPLETED);
    }

    private IndexBuildResult(int successCount, int failureCount, Outcome outcome) {
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
        return new IndexBuildResult(successCount, 0, Outcome.COMPLETED);
    }

    public static IndexBuildResult claimFailed() {
        return new IndexBuildResult(0, 0, Outcome.CLAIM_FAILED);
    }

    public static IndexBuildResult staleGeneration() {
        return new IndexBuildResult(0, 0, Outcome.STALE_GENERATION);
    }

    public static IndexBuildResult rejected() {
        return new IndexBuildResult(0, 0, Outcome.REJECTED);
    }

    public int successCount() {
        return successCount;
    }

    public int failureCount() {
        return failureCount;
    }

    public Outcome outcome() {
        return outcome;
    }

    public boolean isFullySuccessful() {
        return outcome == Outcome.COMPLETED && failureCount == 0;
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

    public enum Outcome {
        COMPLETED,
        CLAIM_FAILED,
        STALE_GENERATION,
        REJECTED
    }
}
