package org.pangrova.coordinator.query.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.concurrency.NotThreadSafe;
import javax.annotation.concurrent.GuardedBy;

/** Owns per-task report sequencing; only successful required publication advances acceptance. */
@ThreadSafe
final class TaskReportSequences {
    private final ConcurrentHashMap<String, ReportSequenceGate> reportSequenceGates = new ConcurrentHashMap<>();

    ReportSequencePermit reserveSequence(String taskId, long sequence) {
        while (true) {
            ReportSequenceGate gate = reportSequenceGates.computeIfAbsent(
                    taskId, ReportSequenceGate::new);
            SequenceReservation reservation = gate.tryReserve(sequence);
            if (reservation.retired()) {
                reportSequenceGates.remove(taskId, gate);
                continue;
            }
            if (reservation.stale()) {
                return null;
            }
            if (reservation.permit() != null) {
                return reservation.permit();
            }
            reservation.waitFor().join();
        }
    }

    /** Clears sequence tracking for a completed/cancelled task to prevent memory leak. */
    void clearTaskSequence(String taskId) {
        ReportSequenceGate gate = reportSequenceGates.get(taskId);
        if (gate != null && gate.retire()) {
            reportSequenceGates.remove(taskId, gate);
        }
    }

    @NotThreadSafe
    final class ReportSequencePermit implements AutoCloseable {
        private final String taskId;
        private final ReportSequenceGate gate;
        private final long sequence;
        private boolean closed;

        private ReportSequencePermit(
                String taskId, ReportSequenceGate gate, long sequence) {
            this.taskId = taskId;
            this.gate = gate;
            this.sequence = sequence;
        }

        void accept() {
            gate.accept(sequence);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (gate.release(sequence)) {
                reportSequenceGates.remove(taskId, gate);
            }
        }
    }

    @ThreadSafe
    private final class ReportSequenceGate {
        private final String taskId;
        @GuardedBy("this")
        private long acceptedSequence = -1L;
        @GuardedBy("this")
        private long highestSeenSequence = -1L;
        @GuardedBy("this")
        private long activeSequence = -1L;
        @GuardedBy("this")
        private CompletableFuture<Void> activeCompletion;
        @GuardedBy("this")
        private Thread activeOwner;
        @GuardedBy("this")
        private boolean retired;

        private ReportSequenceGate(String taskId) {
            this.taskId = taskId;
        }

        private synchronized SequenceReservation tryReserve(long sequence) {
            if (retired) {
                return SequenceReservation.retiredReservation();
            }
            if (activeCompletion != null) {
                if (activeOwner == Thread.currentThread()) {
                    throw new IllegalStateException("Report publication cannot join its own sequence");
                }
                return SequenceReservation.waiting(activeCompletion);
            }
            if (sequence <= acceptedSequence || sequence < highestSeenSequence) {
                return SequenceReservation.staleReservation();
            }
            var completion = new CompletableFuture<Void>();
            var reservation = SequenceReservation.acquired(new ReportSequencePermit(taskId, this, sequence));
            highestSeenSequence = sequence;
            activeSequence = sequence;
            activeCompletion = completion;
            activeOwner = Thread.currentThread();
            return reservation;
        }

        private synchronized void accept(long sequence) {
            requireActive(sequence);
            acceptedSequence = sequence;
        }

        private boolean release(long sequence) {
            CompletableFuture<Void> completion;
            boolean remove;
            synchronized (this) {
                requireActive(sequence);
                // A failed durable commit may retry the same sequence, but an
                // older report must not overtake the attempt that reached us.
                completion = activeCompletion;
                activeCompletion = null;
                activeOwner = null;
                activeSequence = -1L;
                remove = retired;
            }
            completion.complete(null);
            return remove;
        }

        private synchronized boolean retire() {
            retired = true;
            return activeCompletion == null;
        }

        private void requireActive(long sequence) {
            if (activeCompletion == null || activeSequence != sequence) {
                throw new IllegalStateException("report sequence permit is not active");
            }
        }
    }

    private record SequenceReservation(
            ReportSequencePermit permit,
            CompletableFuture<Void> waitFor,
            boolean stale,
            boolean retired) {

        private static SequenceReservation acquired(ReportSequencePermit permit) {
            return new SequenceReservation(permit, null, false, false);
        }

        private static SequenceReservation waiting(CompletableFuture<Void> waitFor) {
            return new SequenceReservation(null, waitFor, false, false);
        }

        private static SequenceReservation staleReservation() {
            return new SequenceReservation(null, null, true, false);
        }

        private static SequenceReservation retiredReservation() {
            return new SequenceReservation(null, null, false, true);
        }
    }

}
