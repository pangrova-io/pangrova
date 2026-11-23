package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskAdmissionReason;
import org.pangrova.worker.exception.TaskAdmissionException;
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
 * Sole process-local admission authority, retaining compact identities until their deadline. Task
 * resource cleanup cannot reopen an admitted attempt. Locks protect only in-memory identities;
 * resource reservation, execution and waiting happen outside the registry lock.
 */
@ThreadSafe
final class TaskAdmissionRegistry {
    private static final int ADMISSION_DRAIN_TIMEOUT_SECONDS = 30;
    private final Map<String, Entry> entries = new HashMap<>();
    private final int capacity;
    private final Clock clock;
    private boolean closed;

    TaskAdmissionRegistry(int capacity, Clock clock) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Admission capacity must be positive");
        }
        this.capacity = capacity;
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    Reservation reserve(TaskRequest request, String workerTaskId) {
        return reserve(request.getAttempt(), request.getDeadlineEpochMs(),
                CanonicalTaskAdmissionSeals.digest(request), workerTaskId, false);
    }

    Reservation reserve(TaskAdmissionSealRequest authority, String workerTaskId) {
        CanonicalTaskAdmissionSeals.validate(authority, authority.getAttempt().getTaskId());
        return reserve(authority.getAttempt(), authority.getDeadlineEpochMs(),
                authority.getCanonicalRequestChecksum().getDigest().toByteArray(), workerTaskId, true);
    }

    private Reservation reserve(TaskAttemptId attempt, long deadline, byte[] digest, String workerTaskId,
                                boolean preparation) {
        requireUnexpired(deadline);
        String taskId = attempt.getTaskId();
        if (taskId.isBlank()) throw new IllegalArgumentException("Canonical task identity is required");
        synchronized (entries) {
            requireOpen();
            requireUnexpired(deadline);
            Entry current = entries.get(taskId);
            if (current != null) {
                if (!MessageDigest.isEqual(current.digest, digest)) {
                    throw new TaskAdmissionException(
                            TaskAdmissionReason.IDENTITY_CONFLICT,
                            "Canonical task identity was reused with a different request: "
                                    + taskId);
                }
                if (current.sealed) {
                    throw new TaskAdmissionException(
                            TaskAdmissionReason.SEALED,
                            "Exact task admission is sealed: " + taskId);
                }
                return new Reservation(taskId, current, false);
            }
            ensureCapacity();
            Entry added =
                    new Entry(
                            digest,
                            workerTaskId,
                            deadline,
                            attempt, preparation);
            Reservation reservation = new Reservation(taskId, added, true);
            entries.put(taskId, added);
            return reservation;
        }
    }

    Optional<PreparationTerminal> preparationTerminal(TaskAdmissionSealRequest authority) {
        synchronized (entries) {
            Entry entry = entries.get(authority.getAttempt().getTaskId());
            if (entry == null || !entry.preparation) return Optional.empty();
            requireSealMatch(entry, authority);
            return Optional.ofNullable(entry.preparationTerminal);
        }
    }

    boolean isPreparation(TaskAdmissionSealRequest authority) {
        synchronized (entries) {
            Entry entry = entries.get(authority.getAttempt().getTaskId());
            if (entry == null || !entry.preparation) return false;
            requireSealMatch(entry, authority);
            return true;
        }
    }

    void preparationFinished(TaskAdmissionSealRequest authority, TaskReport receipt,
                             org.pangrova.protocol.v2.SourcePreparationResult result) {
        TaskReport compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(receipt);
        synchronized (entries) {
            Entry entry = entries.get(authority.getAttempt().getTaskId());
            if (entry == null || !entry.preparation) throw new IllegalStateException("Preparation admission is absent");
            requireSealMatch(entry, authority);
            entry.sealed = true;
            entry.noOwnerExited = true;
            entry.drainReceipt = compact;
            entry.preparationTerminal = new PreparationTerminal(
                    result.getUsage(), result.getError(), result.hasSource());
        }
    }

    record PreparationTerminal(org.pangrova.protocol.v2.ResourceUsage usage,
                               org.pangrova.protocol.v2.ExecutionError error, boolean succeeded) {}

    boolean knownIdentity(TaskAdmissionSealRequest request) {
        synchronized (entries) {
            Entry entry = entries.get(request.getAttempt().getTaskId());
            if (entry == null) {
                return false;
            }
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
                if (mustExist) {
                    throw new TaskAdmissionException(
                            TaskAdmissionReason.IDENTITY_CONFLICT,
                            "Known exact admission was retired before its seal: " + taskId);
                }
                // Do not retain arbitrary expired identities or invent a lost historical receipt.
                if (request.getDeadlineEpochMs() <= clock.millis()) {
                    return new SealStatus("", false, Optional.empty());
                }
                ensureCapacity();
                entry = new Entry(digest, "", request.getDeadlineEpochMs(), request.getAttempt(), false);
                entry.sealed = true;
                entry.noOwnerExited = true;
                entry.admitted.complete("");
                entries.put(taskId, entry);
            } else {
                requireSealMatch(entry, request);
                entry.sealed = true;
            }
            return new SealStatus(
                    entry.handedOff ? entry.workerTaskId : "",
                    entry.noOwnerExited,
                    Optional.ofNullable(entry.drainReceipt));
        }
    }

    private static void requireSealMatch(Entry entry, TaskAdmissionSealRequest request) {
        if (!MessageDigest.isEqual(
                        entry.digest,
                        request.getCanonicalRequestChecksum().getDigest().toByteArray())
                || entry.deadline != request.getDeadlineEpochMs()
                || !entry.attempt.equals(request.getAttempt())) {
            throw new TaskAdmissionException(
                    TaskAdmissionReason.IDENTITY_CONFLICT,
                    "Admission seal does not match the issued task: "
                            + request.getAttempt().getTaskId());
        }
    }

    void retainDrainReceipt(String workerTaskId, TaskReport receipt, long deadline) {
        TaskReport compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(receipt);
        synchronized (entries) {
            Entry entry = entries.get(compact.getAttempt().getTaskId());
            if (entry == null && deadline <= clock.millis()) {
                return;
            }
            if (entry == null
                    || !entry.workerTaskId.equals(workerTaskId)
                    || !entry.attempt.equals(compact.getAttempt())) {
                throw new IllegalStateException(
                        "Drain receipt does not own its exact admitted task");
            }
            entry.drainReceipt = compact;
        }
    }

    private void ensureCapacity() {
        if (entries.size() < capacity) {
            return;
        }
        entries.values()
                .removeIf(
                        entry ->
                                entry.deadline <= clock.millis()
                                        && entry.admitted.isDone()
                                        && (entry.noOwnerExited || entry.drainReceipt != null));
        if (entries.size() >= capacity) {
            throw new TaskAdmissionException(
                    TaskAdmissionReason.CAPACITY,
                    "Task admission identity limit reached: " + capacity);
        }
    }

    record SealStatus(String workerTaskId, boolean noOwnerExited, Optional<TaskReport> receipt) {}

    void requireUnexpired(long deadline) {
        if (deadline <= clock.millis()) {
            throw new TaskAdmissionException(
                    TaskAdmissionReason.EXPIRED, "Task admission deadline has expired");
        }
    }

    String resolve(String canonicalTaskId) {
        synchronized (entries) {
            Entry entry = entries.get(canonicalTaskId);
            return entry == null
                            || entry.workerTaskId.isEmpty()
                            || !entry.handedOff
                    ? null
                    : entry.workerTaskId;
        }
    }

    void removeExpired(String canonicalTaskId, String workerTaskId) {
        synchronized (entries) {
            Entry entry = entries.get(canonicalTaskId);
            if (entry != null
                    && entry.workerTaskId.equals(workerTaskId)
                    && entry.deadline <= clock.millis()
                    && entry.admitted.isDone()
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

    /** Permanently seals new reservations/publication without joining the current callback. */
    void closeAdmissions() {
        synchronized (entries) {
            closed = true;
        }
    }

    void sealAndAwaitAdmissions() {
        CompletableFuture<Void> pending;
        synchronized (entries) {
            closed = true;
            pending =
                    CompletableFuture.allOf(
                            entries.values().stream()
                                    // Handed-off tasks are joined by their lifecycle owners.
                                    .filter(entry -> !entry.handedOff && !entry.admitted.isDone())
                                    .map(entry -> entry.admitted.handle((result, failure) -> null))
                                    .toArray(CompletableFuture[]::new));
        }
        try {
            pending.get(ADMISSION_DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while draining task admissions", failure);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("Task admissions have not physically drained", failure);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new TaskAdmissionException(
                    TaskAdmissionReason.CLOSED, "Task admission is closed");
        }
    }

    final class Reservation implements TaskManager.TaskAdmission {
        private final String taskId;
        private final Entry entry;
        private final boolean first;

        private Reservation(String taskId, Entry entry, boolean first) {
            this.taskId = taskId;
            this.entry = entry;
            this.first = first;
        }

        boolean first() {
            return first;
        }

        String workerTaskId() {
            return entry.workerTaskId;
        }

        @Override
        public boolean completed() {
            return entry.admitted.isDone();
        }

        @Override
        public java.util.concurrent.CompletionStage<String> completion() {
            return entry.admitted.minimalCompletionStage();
        }

        void publish(Runnable publication) {
            synchronized (entries) {
                requireOpen();
                requireUnexpired(entry.deadline);
                requireUnsealed();
                publication.run();
            }
        }

        // Source preparation and pre-start fixtures have no separate endpoint setup.
        void admitted() {
            handOff();
            entry.admitted.complete(entry.workerTaskId);
        }

        void handOff() {
            synchronized (entries) {
                requireOpen();
                requireUnexpired(entry.deadline);
                requireUnsealed();
                entry.handedOff = true;
            }
        }

        void completeTaskSetup() {
            synchronized (entries) {
                requireUnexpired(entry.deadline);
                requireUnsealed();
            }
            entry.admitted.complete(entry.workerTaskId);
        }

        private void requireUnsealed() {
            if (entry.sealed) {
                throw new TaskAdmissionException(
                        TaskAdmissionReason.SEALED,
                        "Exact task admission is sealed: " + taskId);
            }
        }

        void failAfterHandoff(Throwable failure) {
            entry.admitted.completeExceptionally(failure);
        }

        void rejectedBeforeOwnership(Throwable failure) {
            synchronized (entries) {
                entry.noOwnerExited = true;
                if (!entry.sealed) {
                    entries.remove(taskId, entry);
                }
            }
            entry.admitted.completeExceptionally(failure);
        }

        String awaitAdmission() {
            requireUnexpired(entry.deadline);
            try {
                return entry.admitted.get(
                        Math.max(1, entry.deadline - clock.millis()), TimeUnit.MILLISECONDS);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while awaiting task admission: " + taskId, failure);
            } catch (TimeoutException failure) {
                throw new TaskAdmissionException(
                        TaskAdmissionReason.EXPIRED,
                        "Task admission deadline expired while awaiting its owner: " + taskId);
            } catch (ExecutionException failure) {
                if (failure.getCause() instanceof RuntimeException error) {
                    throw error;
                }
                if (failure.getCause() instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(
                        "Task admission failed: " + taskId, failure.getCause());
            }
        }
    }

    private static final class Entry {
        private final byte[] digest;
        private final String workerTaskId;
        private final long deadline;
        private final TaskAttemptId attempt;
        private final boolean preparation;
        private final CompletableFuture<String> admitted = new CompletableFuture<>();
        private boolean sealed;
        private boolean noOwnerExited;
        private boolean handedOff;
        private TaskReport drainReceipt;
        private PreparationTerminal preparationTerminal;

        private Entry(byte[] digest, String workerTaskId, long deadline, TaskAttemptId attempt, boolean preparation) {
            this.digest = digest;
            this.workerTaskId = java.util.Objects.requireNonNull(workerTaskId, "workerTaskId");
            this.deadline = deadline;
            this.attempt = attempt;
            this.preparation = preparation;
        }
    }
}
