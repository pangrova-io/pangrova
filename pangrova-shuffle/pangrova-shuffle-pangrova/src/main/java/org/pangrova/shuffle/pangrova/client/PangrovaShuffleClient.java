package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

import io.grpc.ManagedChannel;

/**
 * gRPC client wrapper for communicating with a remote shuffle server.
 * Provides typed methods for write, commit, and read operations.
 *
 * <p>Thread-safe: gRPC stubs are thread-safe. Each PangrovaShuffleClient wraps
 * a single ManagedChannel to one remote shuffle server endpoint.</p>
 */
public final class PangrovaShuffleClient implements Closeable {

    private final ManagedChannel channel;
    private final ShuffleServiceGrpc.ShuffleServiceBlockingStub blockingStub;
    private final ShuffleServiceGrpc.ShuffleServiceStub asyncStub;
    private final long writeTimeoutMs;
    private final long readTimeoutMs;

    /** Creates a shuffle client over the given channel with write/read timeouts. */
    public PangrovaShuffleClient(ManagedChannel channel, long writeTimeoutMs, long readTimeoutMs) {
        this.channel = channel;
        this.blockingStub = ShuffleServiceGrpc.newBlockingStub(channel);
        this.asyncStub = ShuffleServiceGrpc.newStub(channel);
        this.writeTimeoutMs = writeTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /** Opens one flow-controlled multiplexed stream for a producer and service endpoint. */
    RemoteShuffleProducerStream openProducerStream(
            String shuffleId,
            ExchangeTaskAttempt taskAttempt,
            CompressionCodec codec,
            RenewableShuffleAccess access,
            long taskDeadlineEpochMs) {
        access.currentAtBoundary(taskDeadlineEpochMs);
        long deadline = taskDeadlineEpochMs;
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle task deadline has expired");
        }
        return new RemoteShuffleProducerStream(
                asyncStub.withDeadlineAfter(remaining, TimeUnit.MILLISECONDS),
                shuffleId, taskAttempt, codec, access, deadline);
    }

    static WriteBatchRequest buildWriteRequest(
            String shuffleId,
            ExchangeTaskAttempt taskAttempt,
            WriteFrame frame,
            CompressionCodec codec, ShuffleAccessCapability accessCapability) {
        return WriteBatchRequest.newBuilder()
                .setShuffleId(shuffleId)
                .setPartitionId(frame.partitionId())
                .setStreamSequence(frame.streamSequence())
                .setChunk(frame.chunk())
                .setUncompressedSize(frame.uncompressedSize())
                .setRowCount(frame.rowCount())
                .setCodec(codec)
                .setProducerAttempt(toProtocol(taskAttempt))
                .setAccessCapability(accessCapability)
                .build();
    }

    record WriteFrame(
            int partitionId,
            long streamSequence,
            BatchChunk chunk,
            long uncompressedSize,
            long rowCount) {}

    /**
     * Atomically commits all non-empty partitions written by one producer attempt.
     */
    public CommitProducerAttemptResponse commitProducerAttempt(
            String shuffleId,
            ExchangeTaskAttempt taskAttempt,
            List<PartitionCommitSummary> partitions,
            ShuffleAccessCapability accessCapability,
            long taskDeadlineEpochMs) {
        requireValidCapability(accessCapability);
        long timeout = remainingTimeout(
                taskDeadlineEpochMs, writeTimeoutMs,
                accessCapability.getExpiresAtEpochMs());
        return blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                .commitProducerAttempt(buildCommitRequest(
                        shuffleId, taskAttempt, partitions, accessCapability));
    }

    /**
     * Opens a demand-driven partition stream whose close cancels the gRPC call.
     *
     * @param shuffleId durable shuffle allocation identifier
     * @param context exact consumer attempt and task deadline
     * @param accessCapability operation-scoped read capability
     * @param startSequence first complete batch sequence to read
     * @return a bounded cancellable read stream
     */
    public RemoteShufflePartitionRead openPartitionRead(
            String shuffleId,
            ExchangeChannelContext context,
            ShuffleAccessCapability accessCapability,
            long startSequence) {
        return openPartitionRead(shuffleId, context, accessCapability, startSequence, null);
    }

    RemoteShufflePartitionRead openPartitionRead(String shuffleId, ExchangeChannelContext context,
            ShuffleAccessCapability accessCapability, long startSequence,
            org.pangrova.protocol.v2.ShufflePartitionReadRange range) {
        requireValidCapability(accessCapability);
        long timeout = context.deadlineEpochMillis() - System.currentTimeMillis();
        if (timeout <= 0) {
            throw new RemoteShuffleClientException("Remote shuffle read deadline has expired");
        }
        return new RemoteShufflePartitionRead(
                asyncStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS),
                buildReadRequest(shuffleId, context, accessCapability, startSequence, range));
    }

    /** Renews an operation-scoped data capability for one task attempt. */
    ShuffleAccessCapability renewDataAccess(
            String shuffleId,
            ExchangeTaskAttempt attempt,
            ShuffleAccessOperation operation,
            ShuffleAccessCapability previous,
            long taskDeadlineEpochMs) {
        if (operation != ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ
                && operation != ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE) {
            throw new IllegalArgumentException("Worker may renew only READ or WRITE access");
        }
        requireValidCapability(previous);
        long timeout = remainingTimeout(taskDeadlineEpochMs,
                operation == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ
                        ? readTimeoutMs : writeTimeoutMs,
                previous.getExpiresAtEpochMs());
        RenewAccessRequest request = RenewAccessRequest.newBuilder()
                .setOperation(operation).setShuffleId(shuffleId)
                .setExecutionOwner(toProtocol(attempt).getExecutionOwner())
                .setTaskAttempt(toProtocol(attempt))
                .setPreviousCapability(previous).build();
        ShuffleAccessCapability renewed = blockingStub
                .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                .renewAccess(request).getAccessCapability();
        if (renewed.getToken().isEmpty()
                || renewed.getExpiresAtEpochMs() <= previous.getExpiresAtEpochMs()
                || renewed.getExpiresAtEpochMs() <= System.currentTimeMillis()) {
            throw new RemoteShuffleClientException(
                    "Shuffle renewal returned an invalid or non-advancing capability");
        }
        return renewed;
    }

    static CommitProducerAttemptRequest buildCommitRequest(
            String shuffleId,
            ExchangeTaskAttempt taskAttempt,
            List<PartitionCommitSummary> partitions,
            ShuffleAccessCapability accessCapability) {
        return CommitProducerAttemptRequest.newBuilder()
                .setShuffleId(shuffleId)
                .setProducerAttempt(toProtocol(taskAttempt))
                .addAllPartitions(partitions)
                .setAccessCapability(accessCapability)
                .build();
    }

    static ReadPartitionRequest buildReadRequest(
            String shuffleId,
            ExchangeChannelContext context,
            ShuffleAccessCapability accessCapability) {
        return buildReadRequest(shuffleId, context, accessCapability, 0);
    }

    static ReadPartitionRequest buildReadRequest(
            String shuffleId,
            ExchangeChannelContext context,
            ShuffleAccessCapability accessCapability,
            long startSequence) {
        return buildReadRequest(shuffleId, context, accessCapability, startSequence, null);
    }

    static ReadPartitionRequest buildReadRequest(String shuffleId, ExchangeChannelContext context,
            ShuffleAccessCapability accessCapability, long startSequence,
            org.pangrova.protocol.v2.ShufflePartitionReadRange range) {
        if (startSequence < 0) {
            throw new IllegalArgumentException("shuffle read start sequence is negative");
        }
        var request = ReadPartitionRequest.newBuilder()
                .setShuffleId(shuffleId)
                .setPartitionId(context.exchangePartitionId())
                .setStartSequence(startSequence)
                .setConsumerAttempt(toProtocol(context))
                .setDeadlineEpochMs(context.deadlineEpochMillis())
                .setAccessCapability(accessCapability);
        if (range != null) {
            if (startSequence < range.getStartSequence() || startSequence > range.getEndSequenceExclusive()) {
                throw new IllegalArgumentException("Shuffle read cursor is outside the published range");
            }
            request.setReadRange(range);
        }
        return request.build();
    }

    static org.pangrova.protocol.v2.TaskAttemptId toProtocol(
            ExchangeChannelContext context) {
        return toProtocol(context.taskAttempt());
    }

    static org.pangrova.protocol.v2.TaskAttemptId toProtocol(
            ExchangeTaskAttempt taskAttempt) {
        var owner = taskAttempt.executionOwner();
        return org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                .setQueryOrJobId(taskAttempt.queryId())
                .setStageId(taskAttempt.stageId())
                .setPartitionId(taskAttempt.taskPartitionId())
                .setPlanEpoch(taskAttempt.fence().planEpoch())
                .setAttemptId(taskAttempt.fence().attemptId())
                .setFencingToken(taskAttempt.fence().fencingToken())
                .setTaskId(taskAttempt.taskId())
                .setControlEpoch(taskAttempt.controlEpoch())
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity
                        .newBuilder()
                        .setOwnerId(owner.ownerId())
                        .setOwnerIncarnation(owner.ownerIncarnation())
                        .setFencingToken(owner.fencingToken()))
                .setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId(taskAttempt.workerAdmissionDomainId())
                        .setIncarnationId(taskAttempt.workerIncarnationId()))
                .build();
    }

    private static long remainingTimeout(
            long taskDeadlineEpochMs,
            long configuredTimeoutMillis,
            long capabilityExpiryMillis) {
        long remaining = effectiveDeadline(
                taskDeadlineEpochMs, configuredTimeoutMillis, capabilityExpiryMillis)
                - System.currentTimeMillis();
        if (remaining <= 0) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle task deadline has expired");
        }
        return remaining;
    }

    private static long effectiveDeadline(
            long taskDeadlineEpochMs,
            long configuredTimeoutMillis,
            long capabilityExpiryMillis) {
        long configuredDeadline;
        try {
            configuredDeadline = Math.addExact(
                    System.currentTimeMillis(), configuredTimeoutMillis);
        } catch (ArithmeticException ignored) {
            configuredDeadline = Long.MAX_VALUE;
        }
        return Math.min(
                Math.min(taskDeadlineEpochMs, configuredDeadline),
                capabilityExpiryMillis);
    }

    private static void requireValidCapability(ShuffleAccessCapability capability) {
        if (capability == null || capability.getToken().isEmpty()
                || capability.getExpiresAtEpochMs() <= System.currentTimeMillis()) {
            throw new RemoteShuffleClientException(
                    "External shuffle access capability is missing or expired");
        }
    }

    @Override
    public void close() {
        closeChannel(channel);
    }

    static void closeChannel(ManagedChannel channel) {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new RemoteShuffleClientException("Remote shuffle channel has not terminated");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
            throw new RemoteShuffleClientException("Remote shuffle channel drain interrupted", e);
        }
    }

    /** Actual ManagedChannel termination, independent of cache entries or cleanup futures. */
    public boolean isTerminated() {
        return channel.isTerminated();
    }
}
