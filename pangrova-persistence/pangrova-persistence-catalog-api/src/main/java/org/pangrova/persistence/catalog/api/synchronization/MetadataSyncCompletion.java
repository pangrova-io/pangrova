package org.pangrova.persistence.catalog.api.synchronization;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.SyncResult;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * One immutable completion request. The run ID is retained for outcome confirmation/replay.
 * An absent next checkpoint records diagnostics without changing progress. Only an error-free
 * batch with a source-certified coverage bound may advance progress; local wall time is not a bound.
 */
@Experimental
public record MetadataSyncCompletion(UUID runId, CatalogDeploymentIdentity scope, Optional<Instant> expectedCheckpoint,
        Optional<Instant> nextCheckpoint, SyncResult result) {
    public MetadataSyncCompletion {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(expectedCheckpoint, "expectedCheckpoint");
        Objects.requireNonNull(nextCheckpoint, "nextCheckpoint");
        Objects.requireNonNull(result, "result");
        if (!scope.catalogName().equals(result.catalogName()) || result.syncTime() == null
                || result.syncType() == null || result.syncType().isBlank()
                || result.changesApplied() < 0 || result.errors() < 0 || result.durationMs() < 0) {
            throw new IllegalArgumentException("Invalid synchronization result");
        }
        if (nextCheckpoint.isPresent() && (result.errors() != 0
                || nextCheckpoint.get().isBefore(expectedCheckpoint.orElse(Instant.EPOCH)))) {
            throw new IllegalArgumentException("Failed or backwards synchronization cannot advance a checkpoint");
        }
    }
}
