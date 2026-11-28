package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShufflePartitionReadRange;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.ShuffleAccess;

import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Streams verified external-shuffle chunks without whole-batch JVM materialization. */
@ThreadSafe
public final class RemoteShuffleExchangeSource implements org.pangrova.shuffle.api.worker.ShuffleReader<BatchChunk> {

    private final PangrovaPartitionRead binding;
    private final PangrovaShuffleClient client;
    private final RemoteShuffleClientLease clientLease;
    private final ExchangeChannelContext context;
    private final RenewableShuffleAccess access;
    private final Context.CancellableContext rpcScope = Context.current().fork().withCancellation();
    private final Set<RemoteShufflePartitionRead> ownedReads = ConcurrentHashMap.newKeySet();
    private volatile RemoteShufflePartitionRead read;
    private ShuffleAccessCapability activeCapability;
    private final StreamingBatchChunkValidator validator;
    private final long expectedLast;
    private final long expectedBytes;
    private final long expectedRows;
    private final byte[] expectedManifest;
    private final ShufflePartitionReadRange readRange;
    private RuntimeException readFailure;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Opens a bounded authenticated read on the caller-selected channel. Takes ownership of the
     * supplied non-null lease, releasing it on every construction failure and on successful close.
     * Access is separate from immutable read parameters and is renewed at batch boundaries.
     */
    public RemoteShuffleExchangeSource(
            PangrovaPartitionRead binding, ShuffleAccess readAccess,
            RemoteShuffleClientLease clientLease) {
        this.clientLease = checkNotNull(clientLease, "clientLease must not be null");
        try {
            this.binding = checkNotNull(binding, "binding must not be null");
            checkNotNull(readAccess, "readAccess must not be null");
            this.client = clientLease.client();
            this.context = binding.context();
            var partition = binding.publication();
            long startSequence = partition.getStartSequence();
            validator = new StreamingBatchChunkValidator(startSequence);
            expectedLast = partition.getLastSequence();
            expectedBytes = partition.getTotalBytes();
            expectedRows = partition.getTotalRows();
            expectedManifest = partition.getManifestSha256().toByteArray();
            readRange = partition.hasStartSequence() ? ShufflePartitionReadRange.newBuilder()
                    .setStartSequence(startSequence).setEndSequenceExclusive(expectedLast + 1)
                    .setTotalBytes(expectedBytes).setTotalRows(expectedRows)
                    .setManifestSha256(partition.getManifestSha256()).build() : null;
            ShuffleAccessCapability initial = ShuffleAccessCapability.newBuilder()
                    .setToken(ByteString.copyFrom(readAccess.token()))
                    .setExpiresAtEpochMs(readAccess.expiresAtEpochMs())
                    .build();
            this.access = new RenewableShuffleAccess(
                    client, binding.shuffleId(), context.taskAttempt(),
                    ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ, initial);
            this.activeCapability = currentCapability();
            this.read = openRead(activeCapability, startSequence);
            ownedReads.add(read);
        } catch (RuntimeException | Error failure) {
            rpcScope.cancel(failure);
            try {
                clientLease.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Returns one verified chunk, or {@code null} after a verified terminal frame. */
    public synchronized BatchChunk nextVerifiedChunk() {
        if (closed.get()) {
            throw new RemoteShuffleClientException("External shuffle source was revoked");
        }
        if (finished.get()) {
            return null;
        }
        if (readFailure != null) {
            throw readFailure;
        }
        try {
            return nextFrame();
        } catch (RuntimeException failure) {
            readFailure = failure;
            rpcScope.cancel(failure);
            throw failure;
        }
    }

    private BatchChunk nextFrame() {
        if (!validator.hasPartialBatch()) {
            rotateAtBatchBoundary();
        }
        ReadBatchResponse frame = read.next();
        if (frame == null) {
            throw RemoteShuffleClientException.corruption("partition stream ended before its terminal frame");
        }
        return switch (frame.getFrameCase()) {
            case CHUNK -> verifiedChunk(frame.getChunk(), frame.getRowCount());
            case TERMINAL -> terminalFrame(frame);
            case FRAME_NOT_SET -> throw RemoteShuffleClientException.corruption(
                    "partition stream contains an empty frame");
        };
    }

    /** Returns the last completely verified batch sequence. */
    public synchronized long lastCompletedSequence() {
        return validator.nextBatchSequence() - 1;
    }

    public boolean isFinished() {
        return finished.get();
    }

    private ShuffleAccessCapability currentCapability() {
        Context previous = rpcScope.attach();
        try {
            return access.currentAtBoundary(context.deadlineEpochMillis());
        } finally {
            rpcScope.detach(previous);
        }
    }

    private RemoteShufflePartitionRead openRead(ShuffleAccessCapability capability, long sequence) {
        Context previous = rpcScope.attach();
        try {
            return client.openPartitionRead(binding.shuffleId(), context, capability, sequence, readRange);
        } finally {
            rpcScope.detach(previous);
        }
    }

    private void rotateAtBatchBoundary() {
        ShuffleAccessCapability next = currentCapability();
        if (next.equals(activeCapability)) {
            return;
        }
        var replacement = openRead(next, validator.nextBatchSequence());
        ownedReads.add(replacement);
        var previous = read;
        read = replacement;
        activeCapability = next;
        previous.close();
        ownedReads.remove(previous);
    }

    private BatchChunk verifiedChunk(BatchChunk chunk, long rowCount) {
        if (chunk.getBatchSequence() > expectedLast
                || (chunk.getChunkIndex() == 0
                        && chunk.getBatchSizeBytes() > expectedBytes - validator.totalBytes())
                || chunk.getData().size() > expectedBytes - validator.totalBytes()
                || rowCount > expectedRows - validator.totalRows()) {
            throw RemoteShuffleClientException.corruption("read exceeds coordinator publication");
        }
        validator.accept(chunk, rowCount);
        return chunk;
    }

    private BatchChunk terminalFrame(ReadBatchResponse frame) {
        if (frame.getRowCount() != 0) {
            throw RemoteShuffleClientException.corruption("terminal frame row count must be zero");
        }
        ReadPartitionTerminal terminal = frame.getTerminal();
        validateTerminal(terminal);
        ReadBatchResponse afterTerminal = read.next();
        if (afterTerminal != null) {
            throw RemoteShuffleClientException.corruption(
                    afterTerminal.hasTerminal()
                            ? "partition stream contains duplicate terminal frames"
                            : "partition stream contains data after its terminal frame");
        }
        finished.set(true);
        return null;
    }

    private void validateTerminal(ReadPartitionTerminal terminal) {
        if (validator.hasPartialBatch()
                || terminal.getCommittedLastSequence() != validator.nextBatchSequence() - 1) {
            throw RemoteShuffleClientException.corruption("terminal frame does not match complete batches");
        }
        if (terminal.getTotalBytes() != validator.totalBytes()
                || terminal.getTotalRows() != validator.totalRows()) {
            throw RemoteShuffleClientException.corruption("terminal partition totals do not match received data");
        }
        if (terminal.getGenerationPlanEpoch() != binding.planEpoch()
                || terminal.getCommittedProducerCount() != binding.expectedProducerPartitions()) {
            throw RemoteShuffleClientException.corruption(
                    "terminal generation or producer count does not match binding");
        }
        requireExpectedChecksum(
                terminal.getProducerSetChecksum(),
                binding.expectedProducerSetChecksum(),
                "producer set");
        if (terminal.hasCertifiedStartSequence() != (readRange != null)
                || (readRange != null && terminal.getCertifiedStartSequence() != readRange.getStartSequence())
                || terminal.getCommittedLastSequence() != expectedLast
                || terminal.getTotalBytes() != expectedBytes || terminal.getTotalRows() != expectedRows) {
            throw RemoteShuffleClientException.corruption("terminal totals do not match coordinator publication");
        }
        requireExpectedChecksum(terminal.getOutputChecksum(), expectedManifest, "coordinator publication");
        validator.validateOutputChecksum(terminal.getOutputChecksum());
    }

    @Override
    public void close() {
        revoke();
        // Cancellation happens before joining the reader. It wakes both a blocked renewal
        // and a blocked partition stream, including an RPC being installed during rotation.
        synchronized (this) {
            closeReads();
            clientLease.close();
        }
    }

    private void closeReads() {
        RuntimeException failure = null;
        for (RemoteShufflePartitionRead owned : ownedReads) {
            try {
                owned.close();
                ownedReads.remove(owned);
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) throw failure;
    }

    /** Stops new reads without treating cancellation as a verified terminal. */
    public void revoke() {
        closed.set(true);
        rpcScope.cancel(null);
        RuntimeException failure = null;
        for (RemoteShufflePartitionRead owned : ownedReads) {
            try {
                owned.revoke();
            } catch (RuntimeException revokeFailure) {
                if (failure == null) failure = revokeFailure;
                else failure.addSuppressed(revokeFailure);
            }
        }
        if (failure != null) throw failure;
    }

    private static void requireExpectedChecksum(
            Checksum checksum, byte[] expected, String description) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32
                || !MessageDigest.isEqual(checksum.getDigest().toByteArray(), expected)) {
            throw RemoteShuffleClientException.corruption(description + " checksum is invalid");
        }
    }

}
