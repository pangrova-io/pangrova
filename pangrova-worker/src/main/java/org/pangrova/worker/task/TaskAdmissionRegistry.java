package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;

import java.security.MessageDigest;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sole process-local admission authority, retaining compact identities until their deadline.
 * Task resource cleanup cannot reopen an admitted attempt. Locks protect only in-memory
 * identities; resource reservation, execution and waiting happen outside the registry lock.
 */
@ThreadSafe
final class TaskAdmissionRegistry {
    private final Map<String, Entry> entries = new HashMap<>();
    private final int capacity;
    private final Clock clock;
    private boolean closed;

    TaskAdmissionRegistry(int capacity, Clock clock) {
        if (capacity <= 0) throw new IllegalArgumentException("Admission capacity must be positive");
        this.capacity = capacity;
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    Reservation reserve(TaskRequest request, String workerTaskId) {
        requireUnexpired(request.getDeadlineEpochMs());
        String taskId = request.getAttempt().getTaskId();
        if (taskId.isBlank()) throw new IllegalArgumentException("Canonical task identity is required");
        byte[] digest = CanonicalTaskAdmissionSeals.digest(request);
        synchronized (entries) {
            requireOpen();
            requireUnexpired(request.getDeadlineEpochMs());
            Entry current = entries.get(taskId);
            if (current != null) {
                if (!MessageDigest.isEqual(current.digest, digest)) {
                    throw new TaskAdmissionException(TaskAdmissionException.Reason.IDENTITY_CONFLICT,
                            "Canonical task identity was reused with a different request: " + taskId);
                }
                if (current.sealed) throw new TaskAdmissionException(TaskAdmissionException.Reason.SEALED,
                        "Exact task admission is sealed: " + taskId);
                return new Reservation(taskId, current, false);
            }
            ensureCapacity();
            Entry added = new Entry(digest, workerTaskId, request.getDeadlineEpochMs(), request.getAttempt());
            Reservation reservation = new Reservation(taskId, added, true);
            entries.put(taskId, added);
            return reservation;
        }
    }

    boolean knownIdentity(TaskAdmissionSealRequest request) {
        synchronized (entries) {
            Entry entry = entries.get(request.getAttempt().getTaskId());
            if (entry == null) return false;
            requireSealMatch(entry, request);
            return true;
        }
    }

    SealStatus seal(TaskAdmissionSealRequest request, boolean mustExist) {
        String taskId = request.getAttempt().getTaskId();
        byte[] digest = request.getCanonicalRequestChecksum().getDigest().toByteArray();
        synchronized (entries) {
            Entry entry = entries.get(taskId);
            if (entry == null) {
                if (mustExist) throw new TaskAdmissionException(TaskAdmissionException.Reason.IDENTITY_CONFLICT,
                        "Known exact admission was retired before its seal: " + taskId);
                // Do not retain arbitrary expired identities or invent a lost historical receipt.
                if (request.getDeadlineEpochMs() <= clock.millis()) {
                    return new SealStatus("", false, Optional.empty());
                }
                ensureCapacity();
                entry = new Entry(digest, "", request.getDeadlineEpochMs(), request.getAttempt());
                entry.sealed = true;
                entry.noOwnerExited = true;
                entry.admitted.complete("");
                entries.put(taskId, entry);
            } else {
                requireSealMatch(entry, request);
                entry.sealed = true;
            }
            return new SealStatus(entry.handedOff ? entry.workerTaskId : "", entry.noOwnerExited,
                    Optional.ofNullable(entry.drainReceipt));
        }
    }

    private static void requireSealMatch(Entry entry, TaskAdmissionSealRequest request) {
        if (!MessageDigest.isEqual(entry.digest, request.getCanonicalRequestChecksum().getDigest().toByteArray())
                || entry.deadline != request.getDeadlineEpochMs() || !entry.attempt.equals(request.getAttempt())) {
            throw new TaskAdmissionException(TaskAdmissionException.Reason.IDENTITY_CONFLICT,
                    "Admission seal does not match the issued task: " + request.getAttempt().getTaskId());
        }
    }

    void retainDrainReceipt(String workerTaskId, TaskReport receipt, long deadline) {
        TaskReport compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(receipt);
        synchronized (entries) {
            Entry entry = entries.get(compact.getAttempt().getTaskId());
            if (entry == null && deadline <= clock.millis()) return;
            if (entry == null || !entry.workerTaskId.equals(workerTaskId)
                    || !entry.attempt.equals(compact.getAttempt())) {
                throw new IllegalStateException("Drain receipt does not own its exact admitted task");
            }
            entry.drainReceipt = compact;
        }
    }

    private void ensureCapacity() {
        if (entries.size() < capacity) return;
        entries.values().removeIf(entry -> entry.deadline <= clock.millis()
                && entry.admitted.isDone() && (entry.noOwnerExited || entry.drainReceipt != null));
        if (entries.size() >= capacity) {
            throw new TaskAdmissionException(TaskAdmissionException.Reason.CAPACITY,
                    "Task admission identity limit reached: " + capacity);
        }
    }

    record SealStatus(String workerTaskId, boolean noOwnerExited, Optional<TaskReport> receipt) {}

    void requireUnexpired(long deadline) {
        if (deadline <= clock.millis()) {
            throw new TaskAdmissionException(TaskAdmissionException.Reason.EXPIRED,
                    "Task admission deadline has expired");
        }
    }

    String resolve(String canonicalTaskId) {
        synchronized (entries) {
            Entry entry = entries.get(canonicalTaskId);
            return entry == null || entry.workerTaskId.isEmpty() || !entry.admitted.isDone()
                    || entry.admitted.isCompletedExceptionally() ? null : entry.workerTaskId;
        }
    }

    void removeExpired(String canonicalTaskId, String workerTaskId) {
        synchronized (entries) {
            Entry entry = entries.get(canonicalTaskId);
            if (entry != null && entry.workerTaskId.equals(workerTaskId)
                    && entry.deadline <= clock.millis() && entry.admitted.isDone()
                    && (entry.noOwnerExited || entry.drainReceipt != null)) {
                entries.remove(canonicalTaskId, entry);
            }
        }
    }

    int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    void sealAndAwaitAdmissions() {
        CompletableFuture<Void> pending;
        synchronized (entries) {
            closed = true;
            pending = CompletableFuture.allOf(entries.values().stream()
                    .filter(entry -> !entry.admitted.isDone())
                    .map(entry -> entry.admitted.handle((result, failure) -> null))
                    .toArray(CompletableFuture[]::new));
        }
        try {
            pending.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while draining task admissions", failure);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("Task admissions have not physically drained", failure);
        }
    }

    private void requireOpen() {
        if (closed) throw new TaskAdmissionException(TaskAdmissionException.Reason.CLOSED,
                "Task admission is closed");
    }

    final class Reservation {
        private final String taskId;
        private final Entry entry;
        private final boolean first;

        private Reservation(String taskId, Entry entry, boolean first) {
            this.taskId = taskId;
            this.entry = entry;
            this.first = first;
        }

        boolean first() { return first; }

        void publish(Runnable publication) {
            synchronized (entries) {
                requireOpen();
                requireUnexpired(entry.deadline);
                if (entry.sealed) throw new TaskAdmissionException(TaskAdmissionException.Reason.SEALED,
                        "Exact task admission is sealed: " + taskId);
                publication.run();
            }
        }

        void admitted() {
            synchronized (entries) {
                requireOpen();
                requireUnexpired(entry.deadline);
                if (entry.sealed) throw new TaskAdmissionException(TaskAdmissionException.Reason.SEALED,
                        "Exact task admission is sealed: " + taskId);
                entry.handedOff = true;
            }
            entry.admitted.complete(entry.workerTaskId);
        }

        void rejectedBeforeOwnership(Throwable failure) {
            synchronized (entries) {
                entry.noOwnerExited = true;
                if (!entry.sealed) entries.remove(taskId, entry);
            }
            entry.admitted.completeExceptionally(failure);
        }

        String awaitAdmission() {
            requireUnexpired(entry.deadline);
            try {
                return entry.admitted.get(Math.max(1, entry.deadline - clock.millis()), TimeUnit.MILLISECONDS);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while awaiting task admission: " + taskId, failure);
            } catch (TimeoutException failure) {
                throw new TaskAdmissionException(TaskAdmissionException.Reason.EXPIRED,
                        "Task admission deadline expired while awaiting its owner: " + taskId);
            } catch (ExecutionException failure) {
                if (failure.getCause() instanceof RuntimeException error) throw error;
                if (failure.getCause() instanceof Error error) throw error;
                throw new IllegalStateException("Task admission failed: " + taskId, failure.getCause());
            }
        }
    }

    private static final class Entry {
        private final byte[] digest;
        private final String workerTaskId;
        private final long deadline;
        private final TaskAttemptId attempt;
        private final CompletableFuture<String> admitted = new CompletableFuture<>();
        private boolean sealed;
        private boolean noOwnerExited;
        private boolean handedOff;
        private TaskReport drainReceipt;

        private Entry(byte[] digest, String workerTaskId, long deadline, TaskAttemptId attempt) {
            this.digest = digest;
            this.workerTaskId = java.util.Objects.requireNonNull(workerTaskId, "workerTaskId");
            this.deadline = deadline;
            this.attempt = attempt;
        }
    }
}
