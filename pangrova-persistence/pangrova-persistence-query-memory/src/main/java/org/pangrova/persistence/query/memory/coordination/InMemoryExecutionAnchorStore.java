package org.pangrova.persistence.query.memory.coordination;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Single-process execution-anchor store for standalone deployments and tests. */
@ThreadSafe
public final class InMemoryExecutionAnchorStore implements ExecutionAnchorStore {

    private final Map<String, Row> rows = new HashMap<>();
    private final AtomicLong tokenSequence = new AtomicLong();
    private final Clock clock;

    public InMemoryExecutionAnchorStore() {
        this(Clock.systemUTC());
    }

    /** Creates a store using an explicit authoritative clock. */
    public InMemoryExecutionAnchorStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public synchronized Optional<ExecutionAnchor> tryAcquire(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration) {
        requireRequest(executionId, ownerId, ownerIncarnation, leaseDuration);
        Instant now = now();
        Row existing = rows.get(executionId);
        if (existing != null && existing.expiresAt.isAfter(now)) {
            return Optional.empty();
        }
        Row acquired = new Row(
                executionId, ownerId, ownerIncarnation,
                nextToken(), now, now.plusMillis(leaseDuration.toMillis()));
        rows.put(executionId, acquired);
        return Optional.of(acquired.toAnchor());
    }

    @Override
    public synchronized List<ExecutionAnchor> renewOwned(
            String ownerId, String ownerIncarnation, Duration leaseDuration) {
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
        Instant now = now();
        Instant newDeadline = now.plusMillis(leaseDuration.toMillis());
        List<ExecutionAnchor> renewed = new ArrayList<>();
        for (Map.Entry<String, Row> entry : rows.entrySet()) {
            Row row = entry.getValue();
            if (row.matchesOwner(ownerId, ownerIncarnation)
                    && row.expiresAt.isAfter(now)) {
                Row replacement = row.withDeadline(newDeadline);
                entry.setValue(replacement);
                renewed.add(replacement.toAnchor());
            }
        }
        return List.copyOf(renewed);
    }

    @Override
    public synchronized boolean release(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor must not be null");
        Row current = rows.get(anchor.executionId());
        return current != null
                && current.matches(anchor)
                && rows.remove(anchor.executionId(), current);
    }

    @Override
    public synchronized boolean yield(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor must not be null");
        Row current = rows.get(anchor.executionId());
        return current != null
                && current.matches(anchor)
                && rows.replace(anchor.executionId(), current, current.withDeadline(now()));
    }

    @Override
    public synchronized int yieldOwned(String ownerId, String ownerIncarnation) {
        requireOwner(ownerId, ownerIncarnation);
        Instant now = now();
        int yielded = 0;
        for (Map.Entry<String, Row> entry : rows.entrySet()) {
            Row row = entry.getValue();
            if (row.matchesOwner(ownerId, ownerIncarnation)) {
                entry.setValue(row.withDeadline(now));
                yielded++;
            }
        }
        return yielded;
    }

    @Override
    public synchronized List<ExecutionAnchor> claimExpired(
            String newOwnerId,
            String newOwnerIncarnation,
            Duration leaseDuration,
            int maxBatchSize) {
        requireOwner(newOwnerId, newOwnerIncarnation);
        requireDuration(leaseDuration);
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        Instant now = now();
        Instant deadline = now.plusMillis(leaseDuration.toMillis());
        List<Row> expired = rows.values().stream()
                .filter(row -> !row.expiresAt.isAfter(now))
                .sorted(Comparator.comparing((Row row) -> row.expiresAt)
                        .thenComparing(row -> row.executionId))
                .limit(maxBatchSize)
                .toList();
        List<ExecutionAnchor> claimed = new ArrayList<>(expired.size());
        for (Row row : expired) {
            Row replacement = new Row(
                    row.executionId, newOwnerId, newOwnerIncarnation,
                    nextToken(), now, deadline);
            rows.put(row.executionId, replacement);
            claimed.add(replacement.toAnchor());
        }
        return List.copyOf(claimed);
    }

    @Override
    public synchronized Optional<ExecutionAnchor> current(String executionId) {
        requireNonBlank(executionId, "executionId");
        Row row = rows.get(executionId);
        return row != null && row.expiresAt.isAfter(now())
                ? Optional.of(row.toAnchor()) : Optional.empty();
    }

    private long nextToken() {
        return tokenSequence.incrementAndGet();
    }

    private Instant now() {
        return Instant.ofEpochMilli(clock.instant().toEpochMilli());
    }

    private static void requireRequest(
            String executionId, String ownerId, String ownerIncarnation,
            Duration leaseDuration) {
        requireNonBlank(executionId, "executionId");
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
    }

    private static void requireOwner(String ownerId, String ownerIncarnation) {
        requireNonBlank(ownerId, "ownerId");
        requireNonBlank(ownerIncarnation, "ownerIncarnation");
    }

    private static void requireDuration(Duration leaseDuration) {
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        if (leaseDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("leaseDuration must be at least one millisecond");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private record Row(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long fencingToken,
            Instant acquiredAt,
            Instant expiresAt) {

        private boolean matches(ExecutionAnchor anchor) {
            return executionId.equals(anchor.executionId())
                    && matchesOwner(anchor.ownerId(), anchor.ownerIncarnation())
                    && fencingToken == anchor.fencingToken();
        }

        private boolean matchesOwner(String expectedOwner, String expectedIncarnation) {
            return ownerId.equals(expectedOwner)
                    && ownerIncarnation.equals(expectedIncarnation);
        }

        private Row withDeadline(Instant deadline) {
            return new Row(executionId, ownerId, ownerIncarnation,
                    fencingToken, acquiredAt, deadline);
        }

        private ExecutionAnchor toAnchor() {
            return new ExecutionAnchor(
                    executionId, ownerId, ownerIncarnation,
                    fencingToken, acquiredAt, expiresAt);
        }
    }
}
