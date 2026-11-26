package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
import org.pangrova.persistence.generation.api.job.SnapshotPinPressure;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.ToDoubleFunction;

/** Cached, epoch-fenced Native ledger pressure. Scrapes never query PostgreSQL or a connector. */
public final class NativeSnapshotPinPressureMetrics implements AutoCloseable {
    private static final Duration MAX_SAMPLE_AGE = Duration.ofMinutes(15);
    private static final String PREFIX = "pangrova.index.snapshot_pin.native.";
    private final Clock clock;
    private final List<ObservableDoubleGauge> registrations = new ArrayList<>();
    private volatile SnapshotPinPressure sample;
    private long epoch = -1;

    public NativeSnapshotPinPressureMetrics(MetricsProvider provider) {
        this(provider, Clock.systemUTC());
    }

    NativeSnapshotPinPressureMetrics(MetricsProvider provider, Clock clock) {
        this.clock = clock;
        try {
            for (SnapshotPinState state : List.of(SnapshotPinState.INTENDED,
                    SnapshotPinState.PINNED, SnapshotPinState.RELEASING)) {
                MetricAttributes attributes = MetricAttributes.of("state", state.name().toLowerCase(Locale.ROOT));
                registrations.add(provider.createObservedGauge(PREFIX + "observed_count_lower_bound",
                        "Bounded unreleased Native pin intent count; exact only when not truncated",
                        "pins", attributes, () -> stateValue(state, row -> row.countLowerBound())));
                registrations.add(provider.createObservedGauge(PREFIX + "truncated",
                        "Native pin intent observation exceeded its per-state cap", "1", attributes,
                        () -> stateValue(state, row -> row.truncated() ? 1.0 : 0.0)));
                registrations.add(provider.createObservedGauge(PREFIX + "oldest_intent_age_seconds",
                        "Age of the oldest unreleased Native pin intent in this state", "s", attributes,
                        () -> oldestAge(state)));
            }
            registrations.add(provider.createObservedGauge(PREFIX + "sample_age_seconds",
                    "Age of last available bounded Native pin ledger observation", "s",
                    MetricAttributes.empty(), this::sampleAge));
        } catch (RuntimeException failure) {
            try {
                close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    public synchronized void beginEpoch(long newEpoch) {
        epoch = newEpoch;
        sample = null;
    }

    /** A late database result from an ended maintenance term cannot republish its gauges. */
    public synchronized void record(long observedEpoch, Optional<SnapshotPinPressure> observation) {
        if (epoch == observedEpoch && epoch >= 0) {
            sample = observation.orElse(null);
        }
    }

    public synchronized void endEpoch() {
        epoch = -1;
        sample = null;
    }

    private OptionalDouble stateValue(SnapshotPinState state,
                                      ToDoubleFunction<SnapshotPinPressure.StateCount> accessor) {
        return freshState(state).map(row -> OptionalDouble.of(accessor.applyAsDouble(row)))
                .orElseGet(OptionalDouble::empty);
    }

    private OptionalDouble oldestAge(SnapshotPinState state) {
        return freshState(state).flatMap(SnapshotPinPressure.StateCount::oldestCreatedAt)
                .map(this::age).orElseGet(OptionalDouble::empty);
    }

    private Optional<SnapshotPinPressure.StateCount> freshState(SnapshotPinState state) {
        SnapshotPinPressure current = sample;
        if (current == null || !isFresh(current.observedAt())) {
            return Optional.empty();
        }
        return current.states().stream().filter(row -> row.state() == state).findFirst();
    }

    private boolean isFresh(Instant observedAt) {
        Duration age = Duration.between(observedAt, clock.instant());
        return !age.isNegative() && age.compareTo(MAX_SAMPLE_AGE) <= 0;
    }

    private OptionalDouble sampleAge() {
        SnapshotPinPressure current = sample;
        return current == null ? OptionalDouble.empty() : age(current.observedAt());
    }

    private OptionalDouble age(Instant observedAt) {
        Duration age = Duration.between(observedAt, clock.instant());
        return age.isNegative() ? OptionalDouble.empty() : OptionalDouble.of(age.getSeconds());
    }

    @Override
    public synchronized void close() {
        endEpoch();
        RuntimeException failure = null;
        for (ObservableDoubleGauge registration : registrations) {
            try {
                registration.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        registrations.clear();
    }
}
