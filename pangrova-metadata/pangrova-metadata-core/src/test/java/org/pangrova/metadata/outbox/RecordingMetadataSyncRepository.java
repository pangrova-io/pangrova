package org.pangrova.metadata.outbox;

import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Unit-test recorder only; PostgreSQL integration tests own deployment/concurrency/transaction guarantees. */
final class RecordingMetadataSyncRepository implements MetadataSyncRepository {
    final List<MetadataSyncCompletion> completions = new ArrayList<>();
    private final Map<CatalogDeploymentIdentity, Instant> checkpoints = new HashMap<>();

    @Override
    public Optional<Instant> loadCheckpoint(CatalogDeploymentIdentity scope) {
        return Optional.ofNullable(checkpoints.get(scope));
    }

    @Override
    public void recoverPending(CatalogDeploymentIdentity scope) {
        // Synchronous component recorder; it has no pending requests or crash boundary.
        java.util.Objects.requireNonNull(scope, "scope");
    }

    @Override
    public void complete(MetadataSyncCompletion completion) {
        completions.add(completion);
        completion.nextCheckpoint().ifPresent(value -> checkpoints.put(completion.scope(), value));
    }

    @Override
    public Optional<MetadataSyncCompletion> findCompletion(UUID id) {
        return completions.stream().filter(completion -> completion.runId().equals(id)).findFirst();
    }
}
