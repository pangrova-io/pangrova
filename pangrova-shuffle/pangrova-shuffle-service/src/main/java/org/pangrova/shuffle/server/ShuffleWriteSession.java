package org.pangrova.shuffle.server;

import java.security.MessageDigest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.security.enums.CapabilityOperation;
import org.pangrova.shuffle.transport.CompleteWriteBatch;

import static org.pangrova.shuffle.server.ShuffleBatchStore.WriteGrant;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** Owns immutable PUTs and a bounded, ordered inventory window for one producer RPC. */
@NotThreadSafe
final class ShuffleWriteSession implements AutoCloseable {
    static final int MAX_BATCHES = 128;
    static final long WINDOW_MILLIS = 1_000L;
    private static final long CLOCK_GUARD_MILLIS = 50L;

    private final ShuffleAccessControl access;
    private final ShuffleBatchStore batches;
    private final ShuffleObjectStorage objects;
    private final Clock clock;
    private final long recoveryDelayMillis;
    private final BooleanSupplier cancelled;
    private final Map<BatchIdentity, StoredBatch> pending = new LinkedHashMap<>();
    private String shuffleId;
    private TaskAttemptId attempt;
    private ShuffleAccessCapability verifiedCapability;
    private WriteGrant grant;
    private long validUntilNanos;
    private long validUntilMillis;
    private boolean uncertain;
    private boolean closed;

    ShuffleWriteSession(ShuffleAccessControl access, ShuffleBatchStore batches,
            ShuffleObjectStorage objects, Clock clock, long recoveryDelayMillis, BooleanSupplier cancelled) {
        this.access = access;
        this.batches = batches;
        this.objects = objects;
        this.clock = clock;
        this.recoveryDelayMillis = recoveryDelayMillis;
        this.cancelled = cancelled;
    }

    StoredBatch accept(CompleteWriteBatch batch) {
        requireOpen();
        bindProducer(batch);
        requireCapability(batch.capability());
        var identity = new BatchIdentity(shuffleId, attempt.getPartitionId(), attempt.getAttemptId(),
                batch.outputPartitionId(), batch.batchSequence());
        var next = new StoredBatch(identity, attempt, identity.objectKey(), batch.canonicalSize(),
                batch.rowCount(), batch.checksum());
        StoredBatch replay = pending.get(identity);
        if (replay != null) {
            requireExactReplay(replay, next);
            return replay;
        }
        requireWindow();
        if (identity.outputPartitionId() < 0 || identity.outputPartitionId() >= grant.partitionCount()) {
            throw new SecurityException("producer output does not match shuffle allocation");
        }
        requireOpen();
        // Only a confirmed immutable PUT may enter the inventory. On a transport failure
        // the write reservation remains durable for late object compensation.
        uncertain = true;
        objects.putImmutable(next.objectKey(), batch::openCanonicalStream, next.sizeBytes(), next.checksum());
        uncertain = false;
        pending.put(identity, next);
        return next;
    }

    void finish() {
        requireOpen();
        publishWindow();
        closed = true;
    }

    boolean needsSettlement() {
        return grant != null && !uncertain;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        pending.clear();
        if (needsSettlement()) {
            batches.settleWriteWindow(grant, shuffleId, attempt);
        }
        grant = null;
    }

    private void bindProducer(CompleteWriteBatch batch) {
        if (attempt == null) {
            shuffleId = batch.shuffleId();
            attempt = batch.attempt();
        } else if (!shuffleId.equals(batch.shuffleId()) || !attempt.equals(batch.attempt())) {
            throw new SecurityException("shuffle write stream changed producer identity");
        }
    }

    private void requireCapability(ShuffleAccessCapability capability) {
        if (!capability.equals(verifiedCapability) || capability.getExpiresAtEpochMs() <= clock.millis()) {
            access.verifyTask(CapabilityOperation.WRITE, shuffleId, attempt, capability);
            verifiedCapability = capability;
        }
    }

    private void requireWindow() {
        if (grant == null || pending.size() == MAX_BATCHES || !windowLive()) {
            publishWindow();
            requireOpen();
            long startedMillis = clock.millis();
            long startedNanos = System.nanoTime();
            grant = batches.openWriteWindow(shuffleId, attempt, recoveryDelayMillis);
            long safeDuration = Math.max(0, grant.protectedForMillis() - CLOCK_GUARD_MILLIS);
            validUntilMillis = Math.addExact(startedMillis, safeDuration);
            validUntilNanos = startedNanos + TimeUnit.MILLISECONDS.toNanos(safeDuration);
        }
        if (!windowLive() || verifiedCapability.getExpiresAtEpochMs() <= clock.millis()) {
            throw new SecurityException("shuffle write authority expired during admission");
        }
    }

    private boolean windowLive() {
        return validUntilNanos - System.nanoTime() > 0 && validUntilMillis > clock.millis();
    }

    private void publishWindow() {
        if (!pending.isEmpty()) {
            batches.publishWriteWindow(grant, shuffleId, attempt, new ArrayList<>(pending.values()));
            pending.clear();
            grant = null;
        }
    }

    private void requireOpen() {
        if (closed || uncertain) {
            throw new IllegalStateException("shuffle write stream is closed or has an uncertain PUT");
        }
        if (cancelled.getAsBoolean()) {
            throw new CancellationException("shuffle write stream was cancelled");
        }
    }

    private static void requireExactReplay(StoredBatch previous, StoredBatch next) {
        if (previous.sizeBytes() != next.sizeBytes() || previous.rowCount() != next.rowCount()
                || !MessageDigest.isEqual(previous.checksum(), next.checksum())) {
            throw new IllegalStateException("batch identity conflicts with existing content");
        }
    }
}
