package org.pangrova.server.coordinator.bootstrap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.pangrova.config.enums.ShuffleMode;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.persistence.query.postgresql.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

/** Owns durable shuffle cleanup and write-commit recovery wiring for queries. */
final class QueryDurableRecovery implements AutoCloseable {

    private static final long CALLBACK_DRAIN_TIMEOUT_MS = 5_000;

    private final CoordinatorBootstrapConfig config;
    private final DataSource metadataDataSource;
    private final HighAvailabilityCoordinationService coordination;
    private final org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime callbacks =
            new org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime();

    private JdbcShuffleCleanupIntentStore shuffleCleanupIntents;
    private QueryDispatcher queryDispatcher;

    QueryDurableRecovery(
            CoordinatorBootstrapConfig config,
            DataSource metadataDataSource,
            HighAvailabilityCoordinationService coordination) {
        this.config = config;
        this.metadataDataSource = metadataDataSource;
        this.coordination = coordination;
    }

    void installShuffleCleanup(QueryManager queryManager, QueryDispatcher dispatcher) {
        if (config.shuffleMode() != ShuffleMode.REMOTE) {
            return;
        }
        if (metadataDataSource == null) {
            throw new IllegalStateException(
                    "remote Shuffle requires shared PostgreSQL execution authority");
        }
        shuffleCleanupIntents = new JdbcShuffleCleanupIntentStore(metadataDataSource);
        queryManager.setShuffleCleanupPending(shuffleCleanupIntents::markPending);
        dispatcher.setShuffleCleanupIntents(shuffleCleanupIntents);
        queryDispatcher = dispatcher;
    }

    @Override
    public void close() {
        callbacks.close(CALLBACK_DRAIN_TIMEOUT_MS);
    }

    ShuffleExecutionOwner resolveShuffleOwner(String queryId) {
        if (coordination == null) {
            throw new IllegalStateException("shuffle execution authority is not initialized");
        }
        var anchor = coordination.requireExecutionAnchor(queryId);
        return new ShuffleExecutionOwner(
                anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
    }

    Set<String> recover(
            List<ExecutionAnchor> claimed,
            WriteCommitRecoveryService writeCommitRecoveryService) {
        if (claimed.isEmpty()) {
            return Set.of();
        }
        try (var operation = callbacks.enter()) {
            java.util.concurrent.atomic.AtomicReference<Set<String>> result =
                    new java.util.concurrent.atomic.AtomicReference<>();
            operation.run(() -> result.set(recoverEntered(claimed, writeCommitRecoveryService)));
            return result.get();
        }
    }

    private Set<String> recoverEntered(List<ExecutionAnchor> claimed,
            WriteCommitRecoveryService writeCommitRecoveryService) {
        Set<String> deferred = Set.of();
        if (writeCommitRecoveryService != null) {
            Map<String, Long> tokens = claimed.stream()
                    .collect(Collectors.toUnmodifiableMap(
                            ExecutionAnchor::executionId,
                            ExecutionAnchor::fencingToken));
            deferred = writeCommitRecoveryService.recover(tokens, coordination::validateDurableExecutionToken)
                    .deferredExecutionIds();
        }
        // Write handoff precedes task reconciliation. Shuffle cleanup waits for worker exit proof.
        for (ExecutionAnchor anchor : claimed) {
            if (deferred.contains(anchor.executionId())) {
                coordination.yieldExecution(anchor);
            }
        }
        return deferred;
    }

    boolean releaseRecoveredQuery(ExecutionAnchor claim) {
        boolean remote = shuffleCleanupIntents != null;
        if (remote && shuffleCleanupIntents.prepareProvenRecovery(claim)) {
            var owner = new ShuffleExecutionOwner(claim.ownerId(), claim.ownerIncarnation(), claim.fencingToken());
            queryDispatcher.recoverShuffleCleanup(claim.executionId(), owner);
        }
        return coordination.releaseDrainedRecoveryClaim(claim, remote);
    }
}
