package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.enums.ScheduleState;
import org.pangrova.materialization.api.exception.MaterializationException;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.requireNonNullField;

/**
 * Durable scheduling record of one materialization.
 *
 * <p>The database row is the only scheduling authority; in-memory timers are
 * wake-up hints. Claims are fenced by an expiring lease and a monotonically
 * increasing version so that two schedulers cannot both refresh the same
 * object.
 *
 * @param object owning object
 * @param targetLag freshness target; empty for manual objects
 * @param nextDueAt next refresh deadline
 * @param lastSuccessAt last successful publication instant
 * @param consecutiveFailures consecutive failed attempts
 * @param state scheduling state
 * @param lease current scheduler lease
 * @param version CAS version of this record
 */
public record Schedule(
        MaterializationId object,
        Optional<TargetLag> targetLag,
        Optional<Instant> nextDueAt,
        Optional<Instant> lastSuccessAt,
        int consecutiveFailures,
        ScheduleState state,
        Optional<ScheduleLease> lease,
        long version) {

    public Schedule {
        if (object == null) {
            throw new IllegalArgumentException("object must not be null");
        }
        targetLag = requireNonNullField(targetLag, "targetLag");
        nextDueAt = requireNonNullField(nextDueAt, "nextDueAt");
        lastSuccessAt = requireNonNullField(lastSuccessAt, "lastSuccessAt");
        lease = requireNonNullField(lease, "lease");
        if (consecutiveFailures < 0) {
            throw new IllegalArgumentException("consecutiveFailures must not be negative");
        }
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        if (state == ScheduleState.DISABLED && targetLag.isPresent()) {
            throw new IllegalArgumentException("a disabled schedule cannot carry a target lag");
        }
        if (state == ScheduleState.ACTIVE && (targetLag.isEmpty() || nextDueAt.isEmpty())) {
            throw new IllegalArgumentException("an active schedule requires a target lag and next due instant");
        }
    }

    /**
     * Creates a manual schedule with no automatic refresh.
     *
     * @param object owning object
     * @return disabled schedule at version zero
     */
    public static Schedule manual(MaterializationId object) {
        return new Schedule(object, Optional.empty(), Optional.empty(), Optional.empty(),
                0, ScheduleState.DISABLED, Optional.empty(), 0);
    }

    /**
     * Creates an active schedule.
     *
     * @param object owning object
     * @param lag freshness target
     * @param nextDueAt first deadline
     * @return active schedule at version zero
     */
    public static Schedule scheduled(MaterializationId object, TargetLag lag, Instant nextDueAt) {
        return new Schedule(object, Optional.of(lag), Optional.of(nextDueAt), Optional.empty(),
                0, ScheduleState.ACTIVE, Optional.empty(), 0);
    }

    /**
     * Claims this schedule for a scheduler owner.
     *
     * @param owner scheduler owner
     * @param now claim instant
     * @param leaseDuration lease duration
     * @return claimed schedule with an advanced version
     */
    public Schedule claim(String owner, Instant now, Duration leaseDuration) {
        if (state != ScheduleState.ACTIVE) {
            throw MaterializationException.invalidStateTransition(
                    "cannot claim a " + state + " schedule for " + object.canonicalName());
        }
        if (lease.isPresent() && !lease.get().isExpiredAt(now)) {
            throw MaterializationException.concurrentPublication(
                    "schedule for " + object.canonicalName() + " is already leased to "
                            + lease.get().owner());
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive");
        }
        return new Schedule(object, targetLag, nextDueAt, lastSuccessAt, consecutiveFailures,
                state, Optional.of(new ScheduleLease(owner, now.plus(leaseDuration))), version + 1);
    }

    /**
     * Records a successful publication.
     *
     * @param completedAt publication instant
     * @return updated schedule with the lease released
     */
    public Schedule recordSuccess(Instant completedAt) {
        Optional<Instant> nextDue = targetLag.map(lag -> lag.deadlineFrom(completedAt));
        return new Schedule(object, targetLag, nextDue, Optional.of(completedAt), 0,
                state, Optional.empty(), version + 1);
    }

    /**
     * Records a failed attempt and schedules a retry.
     *
     * @param retryAt next retry instant
     * @return updated schedule with the lease released
     */
    public Schedule recordFailure(Instant retryAt) {
        if (retryAt == null) {
            throw new IllegalArgumentException("retryAt must not be null");
        }
        return new Schedule(object, targetLag, Optional.of(retryAt), lastSuccessAt,
                consecutiveFailures + 1, state, Optional.empty(), version + 1);
    }

    /**
     * Releases the lease and retries later without counting a failure.
     *
     * <p>Used when the refresh could not start because another refresh of the
     * same object already holds the execution slot; that is contention, not a
     * defect of this schedule.
     *
     * @param retryAt next retry instant
     * @return updated schedule with the lease released
     */
    public Schedule defer(Instant retryAt) {
        if (retryAt == null) {
            throw new IllegalArgumentException("retryAt must not be null");
        }
        return new Schedule(object, targetLag, Optional.of(retryAt), lastSuccessAt,
                consecutiveFailures, state, Optional.empty(), version + 1);
    }

    /**
     * Suspends automatic scheduling while retaining the target lag.
     *
     * @return suspended schedule
     */
    public Schedule suspend() {
        if (state != ScheduleState.ACTIVE) {
            throw MaterializationException.invalidStateTransition(
                    "cannot suspend a " + state + " schedule for " + object.canonicalName());
        }
        return new Schedule(object, targetLag, nextDueAt, lastSuccessAt, consecutiveFailures,
                ScheduleState.SUSPENDED, Optional.empty(), version + 1);
    }

    /**
     * Resumes automatic scheduling.
     *
     * @param dueAt first deadline after resumption
     * @return active schedule
     */
    public Schedule resume(Instant dueAt) {
        if (state != ScheduleState.SUSPENDED || targetLag.isEmpty()) {
            throw MaterializationException.invalidStateTransition(
                    "cannot resume a " + state + " schedule without a target lag for "
                            + object.canonicalName());
        }
        if (dueAt == null) {
            throw new IllegalArgumentException("dueAt must not be null");
        }
        return new Schedule(object, targetLag, Optional.of(dueAt), lastSuccessAt,
                consecutiveFailures, ScheduleState.ACTIVE, Optional.empty(), version + 1);
    }

    /**
     * Changes the freshness target of this schedule.
     *
     * <p>A manual schedule becomes active; an active schedule recomputes its
     * deadline from the last success so a shortened lag takes effect
     * immediately; a suspended schedule keeps its suspended state.
     *
     * @param newLag new freshness target
     * @param now observation instant used for a manual schedule
     * @return updated schedule
     */
    public Schedule changeTargetLag(TargetLag newLag, Instant now) {
        TargetLag lag = requireNonNullField(newLag, "targetLag");
        if (state == ScheduleState.DISABLED) {
            return new Schedule(object, Optional.of(lag), Optional.of(lag.deadlineFrom(now)),
                    lastSuccessAt, consecutiveFailures, ScheduleState.ACTIVE, Optional.empty(),
                    version + 1);
        }
        Optional<Instant> nextDue = state == ScheduleState.ACTIVE
                ? Optional.of(lag.deadlineFrom(lastSuccessAt.orElse(now)))
                : Optional.empty();
        return new Schedule(object, Optional.of(lag), nextDue, lastSuccessAt,
                consecutiveFailures, state, lease, version + 1);
    }

    /**
     * Brings the next deadline forward to {@code now} after an upstream publication.
     *
     * <p>Only an unleased active schedule is advanced; an already-due schedule
     * and a leased or suspended schedule are returned unchanged.
     *
     * @param now upstream publication instant
     * @return schedule due immediately, or this schedule when it cannot advance
     */
    public Schedule advanceDueAt(Instant now) {
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        if (state != ScheduleState.ACTIVE || lease.isPresent()
                || nextDueAt.filter(due -> !due.isAfter(now)).isPresent()) {
            return this;
        }
        return new Schedule(object, targetLag, Optional.of(now), lastSuccessAt,
                consecutiveFailures, state, Optional.empty(), version + 1);
    }

    /**
     * Returns whether the schedule is due for a claim.
     *
     * @param now current instant
     * @return true when active, unleased, and due
     */
    public boolean isDueAt(Instant now) {
        return state == ScheduleState.ACTIVE
                && lease.isEmpty()
                && nextDueAt.map(due -> !due.isAfter(now)).orElse(false);
    }
}
