package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.enums.TaskEgressState;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

/** Actual streams, partition statistics and commit lifecycle for one admitted producer. */
final class RemoteShuffleTaskEgress {

    private static final long CLEANUP_GRACE_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final Context.CancellableContext rpcScope;
    private final String shuffleId;
    private final List<ShuffleServerEndpoint> endpoints;
    private final ShuffleServiceAddress commitAddress;
    private final ExchangeTaskAttempt taskAttempt;
    private final long deadlineEpochMillis;
    private final CompressionCodec codec;
    private final ShuffleAccessCapability accessCapability;
    private final TaskAdmissionSealRequest admissionSeal;
    private final RenewableShuffleAccess renewableAccess;
    private final RemoteShuffleClientLease commitLease;
    private final ConcurrentMap<ShuffleServerEndpoint, RemoteShuffleClientLease> endpointLeases =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<ShuffleServerEndpoint, RemoteShuffleProducerStream> streams =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, RemoteShufflePartitionStats> partitions =
            new ConcurrentHashMap<>();
    private final Object lifecycleMonitor = new Object();
    private TaskEgressState lifecycle = TaskEgressState.ACTIVE;
    private int activeWrites;
    private boolean activeFinish;

    RemoteShuffleTaskEgress(Binding binding, RemoteShuffleClientPool clientPool) {
        this.shuffleId = binding.shuffleId();
        this.endpoints = binding.endpoints();
        this.commitAddress = binding.commitAddress();
        this.taskAttempt = binding.taskAttempt();
        this.deadlineEpochMillis = binding.deadlineEpochMillis();
        this.codec = binding.codec();
        this.accessCapability = binding.accessCapability();
        this.admissionSeal = binding.admissionSeal();
        this.commitLease = clientPool.borrow(commitAddress);
        try {
            for (var endpoint : endpoints) {
                endpointLeases.put(endpoint, clientPool.borrow(endpoint));
            }
            this.renewableAccess =
                    new RenewableShuffleAccess(
                            commitLease.client(),
                            shuffleId,
                            taskAttempt,
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE,
                            accessCapability);
            this.rpcScope = Context.current().fork().withCancellation();
        } catch (RuntimeException | Error failure) {
            try {
                releaseClientLeases();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    String queryId() {
        return taskAttempt.queryId();
    }

    String taskId() {
        return taskAttempt.taskId();
    }

    boolean sameIdentity(RemoteShuffleTaskEgress other) {
        return shuffleId.equals(other.shuffleId)
                && endpoints.equals(other.endpoints)
                && commitAddress.equals(other.commitAddress)
                && taskAttempt.equals(other.taskAttempt)
                && deadlineEpochMillis == other.deadlineEpochMillis
                && codec == other.codec
                && accessCapability.equals(other.accessCapability)
                && admissionSeal.equals(other.admissionSeal);
    }

    boolean belongsTo(TaskAdmissionSealRequest expected) {
        return admissionSeal.equals(expected);
    }

    void write(int partitionId, long rowCount, ByteBuffer data) {
        beginWrite();
        try {
            rpcScope.run(() -> writePartition(partitionId, rowCount, data));
        } finally {
            endWrite();
        }
    }

    private void writePartition(int partitionId, long rowCount, ByteBuffer data) {
        ShuffleServerEndpoint endpoint = endpointFor(partitionId);
        RemoteShufflePartitionStats stats =
                partitions.computeIfAbsent(
                        partitionId, ignored -> new RemoteShufflePartitionStats());
        stats.lock();
        try {
            requireWritingAllowed();
            RemoteShuffleProducerStream stream =
                    streams.computeIfAbsent(endpoint, this::openStream);
            if (isAborted()) {
                stream.abort("producer aborted before write");
                throw new RemoteShuffleClientException("Remote shuffle producer is aborted");
            }
            stream.write(partitionId, stats.nextSequence(), rowCount, data);
            stats.record(rowCount, data);
        } finally {
            stats.unlock();
        }
    }

    void finish() {
        beginFinish();
        try {
            awaitWritesBeforeFinish();
            streams.values().forEach(RemoteShuffleProducerStream::finish);
            requireFinishing();
            rpcScope.run(this::commitProducer);
            markFinished();
        } finally {
            synchronized (lifecycleMonitor) {
                activeFinish = false;
                lifecycleMonitor.notifyAll();
            }
        }
        // This runs after activeFinish is cleared and all actual stream terminal
        // callbacks and the blocking commit RPC have completed successfully.
        rpcScope.close();
        releaseClientLeases();
    }

    private void commitProducer() {
        List<PartitionCommitSummary> summaries =
                partitions.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(entry -> entry.getValue().summary(entry.getKey()))
                        .toList();
        long expectedBytes =
                partitions.values().stream().mapToLong(RemoteShufflePartitionStats::bytes).sum();
        CommitProducerAttemptResponse response =
                commitLease
                        .client()
                        .commitProducerAttempt(
                                shuffleId,
                                taskAttempt,
                                summaries,
                                renewableAccess.currentAtBoundary(deadlineEpochMillis),
                                deadlineEpochMillis);
        if (response.getAcceptedPartitions() != summaries.size()
                || response.getTotalBytes() != expectedBytes
                || response.getCommittedAttemptFencingToken()
                        != taskAttempt.fence().fencingToken()) {
            throw new RemoteShuffleClientException(
                    "CommitProducerAttempt response does not match producer summary");
        }
    }

    void abort(String reason) {
        boolean finished;
        synchronized (lifecycleMonitor) {
            finished = lifecycle == TaskEgressState.FINISHED;
            if (!finished) {
                lifecycle = TaskEgressState.ABORTED;
            }
            lifecycleMonitor.notifyAll();
        }
        // Cancel the blocking renewal and commit calls as well as the streaming RPCs.
        // A cancelled commit may already have taken effect on the server.
        rpcScope.cancel(null);
        long cleanupDeadline = System.nanoTime() + CLEANUP_GRACE_NANOS;
        Throwable failure = finished ? null : abortStreams(reason);
        boolean drained = false;
        try {
            awaitOperationsDrained(cleanupDeadline);
            for (var stream : streams.values()) {
                stream.awaitTerminal(cleanupDeadline);
            }
            drained = true;
        } catch (RuntimeException | Error drainFailure) {
            if (failure == null) {
                failure = drainFailure;
            } else {
                failure.addSuppressed(drainFailure);
            }
        }
        if (drained) {
            try {
                releaseClientLeases();
            } catch (RuntimeException | Error leaseFailure) {
                if (failure == null) {
                    failure = leaseFailure;
                } else {
                    failure.addSuppressed(leaseFailure);
                }
            }
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private Throwable abortStreams(String reason) {
        Throwable failure = null;
        for (var stream : streams.values()) {
            try {
                stream.abort(reason);
            } catch (RuntimeException | Error cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        return failure;
    }

    private void awaitOperationsDrained(long cleanupDeadline) {
        synchronized (lifecycleMonitor) {
            while (activeWrites != 0 || activeFinish) {
                long remaining = cleanupDeadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new RemoteShuffleClientException(
                            "Remote shuffle producer operations have not drained");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(lifecycleMonitor, remaining);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new RemoteShuffleClientException(
                            "Remote shuffle operation drain interrupted", failure);
                }
            }
        }
    }

    private RemoteShuffleProducerStream openStream(ShuffleServerEndpoint endpoint) {
        var lease = endpointLeases.get(endpoint);
        if (lease == null) {
            throw new IllegalStateException("Producer endpoint lease was not pre-admitted");
        }
        // All channel owners were admitted before Native task creation.
        return lease.client()
                .openProducerStream(
                        shuffleId, taskAttempt, codec, renewableAccess, deadlineEpochMillis);
    }

    private void releaseClientLeases() {
        Throwable failure = null;
        for (var entry : endpointLeases.entrySet()) {
            try {
                entry.getValue().close();
                endpointLeases.remove(entry.getKey(), entry.getValue());
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        try {
            commitLease.close();
        } catch (RuntimeException | Error cleanup) {
            if (failure == null) {
                failure = cleanup;
            } else {
                failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private ShuffleServerEndpoint endpointFor(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        int low = 0;
        int high = endpoints.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            ShuffleServerEndpoint endpoint = endpoints.get(middle);
            if (partitionId < endpoint.partitionStart()) {
                high = middle - 1;
            } else if (partitionId >= endpoint.partitionEnd()) {
                low = middle + 1;
            } else {
                return endpoint;
            }
        }
        throw new IllegalArgumentException("No shuffle endpoint owns partition " + partitionId);
    }

    private void beginWrite() {
        synchronized (lifecycleMonitor) {
            if (lifecycle != TaskEgressState.ACTIVE) {
                throw new IllegalStateException("Remote shuffle producer is terminal");
            }
            activeWrites++;
        }
    }

    private void endWrite() {
        synchronized (lifecycleMonitor) {
            activeWrites--;
            if (activeWrites == 0) {
                lifecycleMonitor.notifyAll();
            }
        }
    }

    private void beginFinish() {
        synchronized (lifecycleMonitor) {
            if (lifecycle != TaskEgressState.ACTIVE) {
                throw new IllegalStateException("Remote shuffle producer is terminal");
            }
            lifecycle = TaskEgressState.FINISHING;
            activeFinish = true;
        }
    }

    private void awaitWritesBeforeFinish() {
        synchronized (lifecycleMonitor) {
            boolean interrupted = false;
            while (activeWrites > 0 && lifecycle == TaskEgressState.FINISHING) {
                long remaining = deadlineEpochMillis - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new RemoteShuffleClientException(
                            "Remote shuffle producer completion deadline exceeded");
                }
                try {
                    lifecycleMonitor.wait(remaining);
                } catch (InterruptedException failure) {
                    interrupted = true;
                    lifecycle = TaskEgressState.ABORTED;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new RemoteShuffleClientException(
                        "Remote shuffle producer completion interrupted");
            }
            requireFinishingLocked();
        }
    }

    private void requireWritingAllowed() {
        synchronized (lifecycleMonitor) {
            if (lifecycle == TaskEgressState.ABORTED) {
                throw new RemoteShuffleClientException("Remote shuffle producer is aborted");
            }
        }
    }

    private boolean isAborted() {
        synchronized (lifecycleMonitor) {
            return lifecycle == TaskEgressState.ABORTED;
        }
    }

    private void requireFinishing() {
        synchronized (lifecycleMonitor) {
            requireFinishingLocked();
        }
    }

    private void requireFinishingLocked() {
        if (lifecycle != TaskEgressState.FINISHING) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle producer was aborted during completion");
        }
    }

    private void markFinished() {
        synchronized (lifecycleMonitor) {
            requireFinishingLocked();
            lifecycle = TaskEgressState.FINISHED;
        }
    }

    record Binding(
            String shuffleId,
            List<ShuffleServerEndpoint> endpoints,
            ShuffleServiceAddress commitAddress,
            ExchangeTaskAttempt taskAttempt,
            long deadlineEpochMillis,
            CompressionCodec codec,
            ShuffleAccessCapability accessCapability,
            TaskAdmissionSealRequest admissionSeal) {}
}
