package org.pangrova.worker.exchange.remote;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.shuffle.api.ownership.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.registration.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.registration.ShuffleServiceAddress;
import org.pangrova.shuffle.api.exchange.ExchangeAttemptFence;
import org.pangrova.shuffle.api.exchange.ExchangeTaskAttempt;

/** Task-scoped JNI egress for coordinator-allocated external shuffle. */
@ThreadSafe
public final class RemoteShuffleByteEgress implements AutoCloseable {

    private final RemoteShuffleClientPool clientPool;
    private final ConcurrentMap<String, RemoteShuffleTaskEgress> tasks = new ConcurrentHashMap<>();
    private final Set<RemoteShuffleTaskEgress> pendingCandidates = ConcurrentHashMap.newKeySet();

    @GuardedBy("this")
    private boolean closed;

    @GuardedBy("this")
    private int installing;

    /** Creates an egress over the supplied data-plane connection pool. */
    public RemoteShuffleByteEgress(RemoteShuffleClientPool clientPool) {
        this.clientPool = requireNonNull(clientPool, "clientPool must not be null");
    }

    /** Installs the canonical external-shuffle binding before native task creation. */
    public boolean installTask(TaskRequest request) {
        requireNonNull(request, "request must not be null");
        synchronized (this) {
            if (closed) throw new IllegalStateException("Remote shuffle egress is closed");
            if (!hasExternalBinding(request)) return false;
            installing++;
        }
        try {
            return admitCandidate(toTaskEgress(request, clientPool));
        } finally {
            synchronized (this) {
                installing--;
                notifyAll();
            }
        }
    }

    private boolean admitCandidate(RemoteShuffleTaskEgress task) {
        final RemoteShuffleTaskEgress existing;
        final boolean sealed;
        synchronized (this) {
            pendingCandidates.add(task);
            sealed = closed;
            existing = sealed ? null : tasks.putIfAbsent(task.taskId(), task);
            if (!sealed && existing == null) {
                pendingCandidates.remove(task);
                return true;
            }
        }
        // A failed candidate drain remains owned independently of the existing
        // task with the same id. Do not hold the admission lock across RPC joins.
        task.abort(sealed ? "egress closed during admission" : "duplicate task binding");
        pendingCandidates.remove(task);
        if (sealed) throw new IllegalStateException("Remote shuffle egress is closed");
        if (!existing.sameIdentity(task)) {
            throw new RemoteShuffleClientException(
                    "Conflicting external shuffle task binding for taskId=" + task.taskId());
        }
        return true;
    }

    /** Receives one native-serialized partition batch. */
    public void onPartitionBatch(
            String queryId,
            String taskId,
            int partitionId,
            long rowCount,
            ByteBuffer serializedBatch) {
        RemoteShuffleTaskEgress task = requireTask(queryId, taskId);
        requireNonNull(serializedBatch, "serializedBatch must not be null");
        if (!serializedBatch.hasRemaining()) {
            throw new IllegalArgumentException("serializedBatch must not be empty");
        }
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be positive");
        }
        task.write(partitionId, rowCount, serializedBatch.asReadOnlyBuffer());
    }

    /** Finishes endpoint streams and atomically commits this producer attempt. */
    public void onTaskCompleted(String queryId, String taskId) {
        RemoteShuffleTaskEgress task = requireTask(queryId, taskId);
        try {
            task.finish();
            tasks.remove(taskId, task);
        } catch (RuntimeException | Error failure) {
            try {
                task.abort("producer completion failed");
                tasks.remove(taskId, task);
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Aborts all task-local streams after native execution or callback failure. */
    public void onTaskAborted(String queryId, String taskId, String reason) {
        RemoteShuffleTaskEgress task = tasks.get(taskId);
        if (task == null) {
            return;
        }
        if (!task.queryId().equals(queryId)) {
            throw new RemoteShuffleClientException(
                    "External shuffle task identity mismatch for taskId=" + taskId);
        }
        task.abort(reason);
        // Keep the binding registered if abort fails, so a later release can
        // retry closing streams instead of silently losing the cleanup owner.
        tasks.remove(taskId, task);
    }

    /**
     * Joins only resources owned by this exact original NETWORK request, including failed
     * candidates.
     */
    public void releaseTask(TaskRequest request) {
        var seal = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(request);
        String taskId = seal.getAttempt().getTaskId();
        Throwable failure = null;
        try {
            var task = tasks.get(taskId);
            if (task != null && task.belongsTo(seal)) {
                task.abort("exact task released");
                tasks.remove(taskId, task);
            }
        } catch (RuntimeException | Error cleanup) {
            failure = cleanup;
        }
        for (var candidate : pendingCandidates) {
            if (!candidate.belongsTo(seal)) continue;
            try {
                candidate.abort("unpublished task candidate released");
                pendingCandidates.remove(candidate);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    @Override
    public void close() {
        final List<RemoteShuffleTaskEgress> active;
        synchronized (this) {
            closed = true;
        }
        awaitAdmissions();
        synchronized (this) {
            var all = new HashSet<>(tasks.values());
            all.addAll(pendingCandidates);
            active = new ArrayList<>(all);
        }
        Throwable failure = null;
        for (var task : active) {
            try {
                task.abort("egress closed");
                tasks.remove(task.taskId(), task);
                pendingCandidates.remove(task);
            } catch (RuntimeException | Error cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        clientPool.close();
    }

    private void awaitAdmissions() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        synchronized (this) {
            while (installing != 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new RemoteShuffleClientException("Shuffle task admission has not exited");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new RemoteShuffleClientException(
                            "Shuffle task admission drain interrupted", failure);
                }
            }
        }
    }

    private RemoteShuffleTaskEgress requireTask(String queryId, String taskId) {
        RemoteShuffleTaskEgress task = tasks.get(taskId);
        if (task == null || !task.queryId().equals(queryId)) {
            throw new RemoteShuffleClientException(
                    "No installed external shuffle task for queryId="
                            + queryId
                            + " taskId="
                            + taskId);
        }
        return task;
    }

    private static boolean hasExternalBinding(TaskRequest request) {
        return request.hasExecutionOptions()
                && request.getExecutionOptions().hasShuffle()
                && request.getExecutionOptions().getShuffle().hasExecutionBinding();
    }

    private static RemoteShuffleTaskEgress toTaskEgress(
            TaskRequest request, RemoteShuffleClientPool clientPool) {
        if (!request.hasAttempt() || request.getDeadlineEpochMs() == 0) {
            throw new IllegalArgumentException(
                    "External shuffle task requires attempt identity and deadline");
        }
        var attempt = request.getAttempt();
        var shuffle = request.getExecutionOptions().getShuffle();
        ShuffleExecutionBinding binding = shuffle.getExecutionBinding();
        validateBinding(attempt, shuffle.getPartitionCount(), binding);
        ShuffleExecutionOwner owner = toOwner(binding);
        ExchangeTaskAttempt taskAttempt =
                new ExchangeTaskAttempt(
                        attempt.getQueryOrJobId(),
                        attempt.getStageId(),
                        attempt.getPartitionId(),
                        attempt.getTaskId(),
                        attempt.getControlEpoch(),
                        new ExchangeAttemptFence(
                                attempt.getPlanEpoch(),
                                attempt.getAttemptId(),
                                attempt.getFencingToken()),
                        owner,
                        attempt.getWorkerExecutionIdentity().getAdmissionDomainId(),
                        attempt.getWorkerExecutionIdentity().getIncarnationId());
        List<ShuffleServerEndpoint> endpoints =
                binding.getEndpointsList().stream()
                        .map(
                                endpoint ->
                                        new ShuffleServerEndpoint(
                                                endpoint.getHost(),
                                                endpoint.getPort(),
                                                endpoint.getPartitionStart(),
                                                endpoint.getPartitionEnd(),
                                                endpoint.getTlsIdentity()))
                        .sorted(Comparator.comparingInt(ShuffleServerEndpoint::partitionStart))
                        .toList();
        validateCoverage(endpoints, shuffle.getPartitionCount());
        var commit = binding.getCommitAddress();
        return new RemoteShuffleTaskEgress(
                new RemoteShuffleTaskEgress.Binding(
                        binding.getShuffleId(),
                        endpoints,
                        new ShuffleServiceAddress(
                                commit.getHost(), commit.getPort(), commit.getTlsIdentity()),
                        taskAttempt,
                        request.getDeadlineEpochMs(),
                        parseCodec(shuffle.getCompressionCodec()),
                        binding.getWriteCapability(),
                        org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(request)),
                clientPool);
    }

    private static void validateBinding(
            org.pangrova.protocol.v2.TaskAttemptId attempt,
            int partitionCount,
            ShuffleExecutionBinding binding) {
        if (partitionCount <= 0
                || binding.getShuffleId().isBlank()
                || binding.getEndpointsCount() == 0
                || !binding.hasExecutionOwner()
                || !binding.hasCommitAddress()) {
            throw new IllegalArgumentException(
                    "External shuffle binding requires partitions, id, endpoints, commit address and owner");
        }
        if (binding.getPlanEpoch() != attempt.getPlanEpoch()
                || !binding.getExecutionOwner().equals(attempt.getExecutionOwner())) {
            throw new IllegalArgumentException(
                    "External shuffle binding does not match task attempt fence");
        }
        validateCapability(binding.getWriteCapability());
    }

    private static void validateCoverage(
            List<ShuffleServerEndpoint> endpoints, int partitionCount) {
        int cursor = 0;
        for (ShuffleServerEndpoint endpoint : endpoints) {
            if (endpoint.partitionStart() != cursor || endpoint.partitionEnd() > partitionCount) {
                throw new IllegalArgumentException(
                        "Shuffle endpoint ranges must exactly cover [0, partitionCount)");
            }
            cursor = endpoint.partitionEnd();
        }
        if (cursor != partitionCount) {
            throw new IllegalArgumentException(
                    "Shuffle endpoint ranges must exactly cover [0, partitionCount)");
        }
    }

    private static void validateCapability(ShuffleAccessCapability capability) {
        if (capability.getToken().isEmpty()
                || capability.getExpiresAtEpochMs() <= System.currentTimeMillis()) {
            throw new IllegalArgumentException(
                    "External shuffle write capability is missing or expired");
        }
    }

    private static ShuffleExecutionOwner toOwner(ShuffleExecutionBinding binding) {
        var owner = binding.getExecutionOwner();
        return new ShuffleExecutionOwner(
                owner.getOwnerId(), owner.getOwnerIncarnation(), owner.getFencingToken());
    }

    private static CompressionCodec parseCodec(String codec) {
        // The task codec controls the self-described columnar stream. This
        // transport carries its bytes unchanged and never compresses them again.
        return switch (codec) {
            case "none", "lz4", "zstd" -> CompressionCodec.COMPRESSION_CODEC_NONE;
            default -> throw new IllegalArgumentException("Unknown shuffle stream codec: " + codec);
        };
    }
}
