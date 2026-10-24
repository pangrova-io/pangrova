package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.util.Optional;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.MaterializationRepository;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Durable target-lag scheduler.
 *
 * <p>Each tick claims at most one due schedule with a database lease, invokes
 * the refresh callback under a command id derived from the due deadline, and
 * settles the schedule with success or bounded exponential backoff. Deriving
 * the id from the deadline makes a lease-expiry re-claim of the same window
 * replay the recorded command instead of refreshing twice.
 */
public final class MaterializationScheduler {

    private final MaterializationRepository repository;
    private final ScheduledRefresh refresher;
    private final Clock clock;
    private final Settings settings;

    public MaterializationScheduler(MaterializationRepository repository,
            ScheduledRefresh refresher, Clock clock, Settings settings) {
        if (repository == null || refresher == null || clock == null || settings == null) {
            throw new IllegalArgumentException("scheduler collaborators must not be null");
        }
        this.repository = repository;
        this.refresher = refresher;
        this.clock = clock;
        this.settings = settings;
    }

    /**
     * Claims and runs at most one due schedule.
     *
     * @return completed run, or empty when no schedule is due
     */
    public Optional<Run> runOnce() {
        java.time.Instant now = clock.now();
        Optional<Schedule> claimed = repository.claimDueSchedule(
                settings.owner(), now, settings.leaseDuration());
        if (claimed.isEmpty()) {
            return Optional.empty();
        }
        Schedule schedule = claimed.orElseThrow();
        CommandId commandId = commandId(schedule);
        try {
            refresher.refresh(schedule.object(), commandId, TriggerKind.SCHEDULED);
        } catch (RuntimeException failure) {
            recordFailure(schedule, failure);
            return Optional.of(new Run(schedule, false, failure));
        }
        recordSuccess(schedule);
        return Optional.of(new Run(schedule, true, null));
    }

    private void recordSuccess(Schedule claimed) {
        java.time.Instant now = clock.now();
        repository.findSchedule(claimed.object())
                .filter(current -> current.version() == claimed.version())
                .ifPresent(current -> repository.updateSchedule(
                        current.recordSuccess(now), current.version()));
    }

    private void recordFailure(Schedule claimed, RuntimeException failure) {
        java.time.Instant now = clock.now();
        Optional<Schedule> current = repository.findSchedule(claimed.object())
                .filter(found -> found.version() == claimed.version());
        if (current.isEmpty()) {
            return;
        }
        Schedule failed = current.orElseThrow();
        int failures = failed.consecutiveFailures() + 1;
        Schedule retry = failed.recordFailure(now.plus(backoff(failed.object(), failures)));
        repository.updateSchedule(retry, failed.version());
        if (failures >= settings.maxConsecutiveFailures()) {
            repository.updateSchedule(retry.suspend(), retry.version());
        }
    }

    Duration backoff(MaterializationId object, int failures) {
        long base = settings.baseBackoff().toMillis();
        int exponent = Math.min(Math.max(failures - 1, 0), 20);
        long scaled;
        if (base > (Long.MAX_VALUE >> exponent)) {
            scaled = Long.MAX_VALUE;
        } else {
            scaled = base << exponent;
        }
        double jitter = 0.9d + Math.floorMod(
                31L * object.canonicalName().hashCode() + failures, 21) / 100.0d;
        long withJitter = Math.min(settings.maxBackoff().toMillis(),
                (long) Math.min(Long.MAX_VALUE / 2.0, scaled * jitter));
        return Duration.ofMillis(Math.max(1L, withJitter));
    }

    private static CommandId commandId(Schedule schedule) {
        String seed = schedule.object().canonicalName() + ':'
                + schedule.nextDueAt().orElseThrow();
        return new CommandId("sched-" + UUID.nameUUIDFromBytes(
                seed.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Scheduler configuration.
     *
     * @param owner stable scheduler owner identity
     * @param leaseDuration claim lease duration
     * @param baseBackoff first retry delay
     * @param maxBackoff retry delay ceiling
     * @param maxConsecutiveFailures failures after which the schedule is suspended
     */
    public record Settings(
            String owner,
            Duration leaseDuration,
            Duration baseBackoff,
            Duration maxBackoff,
            int maxConsecutiveFailures) {

        public Settings {
            if (owner == null || owner.isBlank()) {
                throw new IllegalArgumentException("owner must not be blank");
            }
            if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
                throw new IllegalArgumentException("leaseDuration must be positive");
            }
            if (baseBackoff == null || baseBackoff.isZero() || baseBackoff.isNegative()) {
                throw new IllegalArgumentException("baseBackoff must be positive");
            }
            if (maxBackoff == null || maxBackoff.compareTo(baseBackoff) < 0) {
                throw new IllegalArgumentException("maxBackoff must not be shorter than baseBackoff");
            }
            if (maxConsecutiveFailures < 1) {
                throw new IllegalArgumentException("maxConsecutiveFailures must be positive");
            }
        }

        /** Production defaults. */
        public static Settings defaults(String owner) {
            return new Settings(owner, Duration.ofMinutes(2), Duration.ofSeconds(15),
                    Duration.ofHours(1), 8);
        }
    }

    /**
     * Outcome of one claimed schedule.
     *
     * @param claimed claimed schedule
     * @param succeeded whether the refresh callback completed
     * @param failure callback failure when unsuccessful
     */
    public record Run(Schedule claimed, boolean succeeded, RuntimeException failure) {
    }
}
