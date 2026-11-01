package org.pangrova.worker.exchange.remote;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.google.protobuf.ByteString;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.api.exchange.ExchangeChannelContext;
import org.pangrova.shuffle.api.exchange.ExternalShuffleTransport;

import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Streams verified external-shuffle chunks without whole-batch JVM materialization. */
@ThreadSafe
public final class RemoteShuffleExchangeSource implements AutoCloseable {

    private final ExternalShuffleTransport binding;
    private final RemoteShuffleClient client;
    private final RemoteShuffleClientLease clientLease;
    private final ExchangeChannelContext context;
    private final RenewableShuffleAccess access;
    private final Object readerOwnership = new Object();
    private final Set<RemoteShufflePartitionRead> ownedReads = ConcurrentHashMap.newKeySet();
    private volatile RemoteShufflePartitionRead read;
    private ShuffleAccessCapability activeCapability;
    private final StreamingBatchChunkValidator validator = new StreamingBatchChunkValidator();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Opens a bounded gRPC read using the binding's exact partition endpoint. */
    public RemoteShuffleExchangeSource(
            ExternalShuffleTransport binding, RemoteShuffleClientLease clientLease) {
        this.binding = checkNotNull(binding, "binding must not be null");
        this.clientLease = checkNotNull(clientLease, "clientLease must not be null");
        this.client = clientLease.client();
        this.context = channelContext(binding);
        var readAccess = binding.readAccess();
        ShuffleAccessCapability initial =
                ShuffleAccessCapability.newBuilder()
                        .setToken(ByteString.copyFrom(readAccess.token()))
                        .setExpiresAtEpochMs(readAccess.expiresAtEpochMs())
                        .build();
        this.access =
                new RenewableShuffleAccess(
                        client,
                        binding.shuffleId(),
                        context.taskAttempt(),
                        ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ,
                        initial);
        try {
            this.activeCapability = access.currentAtBoundary(context.deadlineEpochMillis());
            this.read = client.openPartitionRead(binding.shuffleId(), context, activeCapability, 0);
            ownedReads.add(read);
        } catch (RuntimeException | Error failure) {
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
        if (!validator.hasPartialBatch()) {
            rotateAtBatchBoundary();
        }
        ReadBatchResponse frame = read.next();
        if (frame == null) {
            throw corruption("partition stream ended before its terminal frame");
        }
        return switch (frame.getFrameCase()) {
            case CHUNK -> verifiedChunk(frame.getChunk(), frame.getRowCount());
            case TERMINAL -> terminalFrame(frame);
            case FRAME_NOT_SET -> throw corruption("partition stream contains an empty frame");
        };
    }

    /** Returns the last completely verified batch sequence. */
    public synchronized long lastCompletedSequence() {
        return validator.nextBatchSequence() - 1;
    }

    public boolean isFinished() {
        return finished.get();
    }

    private void rotateAtBatchBoundary() {
        ShuffleAccessCapability next = access.currentAtBoundary(context.deadlineEpochMillis());
        if (next.equals(activeCapability)) {
            return;
        }
        RemoteShufflePartitionRead replacement =
                client.openPartitionRead(
                        binding.shuffleId(), context, next, validator.nextBatchSequence());
        final RemoteShufflePartitionRead previous;
        synchronized (readerOwnership) {
            ownedReads.add(replacement);
            previous = read;
            if (!closed.get()) {
                read = replacement;
                activeCapability = next;
            }
        }
        if (closed.get()) {
            replacement.revoke();
            throw new RemoteShuffleClientException("External shuffle source was revoked");
        }
        previous.close();
        ownedReads.remove(previous);
    }

    private BatchChunk verifiedChunk(BatchChunk chunk, long rowCount) {
        validator.accept(chunk, rowCount);
        return chunk;
    }

    private BatchChunk terminalFrame(ReadBatchResponse frame) {
        if (frame.getRowCount() != 0) {
            throw corruption("terminal frame row count must be zero");
        }
        ReadPartitionTerminal terminal = frame.getTerminal();
        validateTerminal(terminal);
        ReadBatchResponse afterTerminal = read.next();
        if (afterTerminal != null) {
            throw corruption(
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
            throw corruption("terminal frame does not match complete batches");
        }
        if (terminal.getTotalBytes() != validator.totalBytes()
                || terminal.getTotalRows() != validator.totalRows()) {
            throw corruption("terminal partition totals do not match received data");
        }
        if (terminal.getGenerationPlanEpoch() != binding.planEpoch()
                || terminal.getCommittedProducerCount() != binding.expectedProducerPartitions()) {
            throw corruption("terminal generation or producer count does not match binding");
        }
        requireExpectedChecksum(
                terminal.getProducerSetChecksum(),
                binding.expectedProducerSetChecksum(),
                "producer set");
        validator.validateOutputChecksum(terminal.getOutputChecksum());
    }

    @Override
    public void close() {
        revoke();
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
        clientLease.close();
    }

    /** Stops new reads without treating cancellation as a verified terminal. */
    public void revoke() {
        synchronized (readerOwnership) {
            closed.set(true);
        }
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

    private static ExchangeChannelContext channelContext(ExternalShuffleTransport binding) {
        return new ExchangeChannelContext(
                binding.consumerAttempt(), binding.outputPartitionId(), binding.deadlineEpochMs());
    }

    private static void requireExpectedChecksum(
            Checksum checksum, byte[] expected, String description) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32
                || !MessageDigest.isEqual(checksum.getDigest().toByteArray(), expected)) {
            throw corruption(description + " checksum is invalid");
        }
    }

    private static RemoteShuffleClientException corruption(String message) {
        return new RemoteShuffleClientException("External shuffle corruption: " + message);
    }
}
