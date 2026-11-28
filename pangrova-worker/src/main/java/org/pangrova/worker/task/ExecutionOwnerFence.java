package org.pangrova.worker.task;

import org.pangrova.worker.exception.ExecutionOwnerFenceException;

import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Per-execution Worker fence for active-active query and background-job owners.
 *
 * <p>A higher backend-issued token supersedes every older owner. Equal tokens
 * are accepted only for the same exact process incarnation. Retired entries are
 * retained briefly so delayed task messages cannot revive a completed attempt.</p>
 */
final class ExecutionOwnerFence {

    static final int MAX_TRACKED_EXECUTIONS = 100_000;
    private static final long RETENTION_MILLIS = Duration.ofMinutes(10).toMillis();
    private static final long ACTIVE = Long.MAX_VALUE;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    void verify(String executionId, ExecutionOwnerIdentity identity) {
        requireText(executionId, "executionId");
        Objects.requireNonNull(identity, "executionOwner must not be null");
        if (!entries.containsKey(executionId) && entries.size() >= MAX_TRACKED_EXECUTIONS) {
            removeExpired();
            if (entries.size() >= MAX_TRACKED_EXECUTIONS) {
                throw new ExecutionOwnerFenceException(
                        "Execution-owner fence is at capacity");
            }
        }
        entries.compute(executionId, (ignored, current) ->
                accept(identity, current, System.currentTimeMillis()));
    }

    void retire(String executionId) {
        long retireAt = System.currentTimeMillis() + RETENTION_MILLIS;
        entries.computeIfPresent(executionId, (ignored, entry) ->
                entry.withRetireAt(retireAt));
    }

    private Entry accept(
            ExecutionOwnerIdentity identity, Entry current, long now) {
        if (current == null || current.retireAt <= now
                || identity.fencingToken() > current.fencingToken) {
            return new Entry(identity.ownerId(), identity.ownerIncarnation(),
                    identity.fencingToken(), ACTIVE);
        }
        if (identity.fencingToken() < current.fencingToken) {
            throw new ExecutionOwnerFenceException(
                    "Stale execution-owner fencing token: " + identity.fencingToken());
        }
        if (!current.ownerId.equals(identity.ownerId())
                || !current.ownerIncarnation.equals(identity.ownerIncarnation())) {
            throw new ExecutionOwnerFenceException(
                    "Execution-owner identity changed without a higher fencing token");
        }
        return current.withRetireAt(ACTIVE);
    }

    private void removeExpired() {
        long now = System.currentTimeMillis();
        entries.entrySet().removeIf(entry -> entry.getValue().retireAt <= now);
    }

    private record Entry(
            String ownerId,
            String ownerIncarnation,
            long fencingToken,
            long retireAt) {

        private Entry withRetireAt(long deadline) {
            return new Entry(ownerId, ownerIncarnation, fencingToken, deadline);
        }
    }
}
