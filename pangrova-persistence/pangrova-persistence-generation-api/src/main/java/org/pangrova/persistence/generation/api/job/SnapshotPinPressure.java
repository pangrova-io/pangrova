package org.pangrova.persistence.generation.api.job;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded observation of unreleased Native pin intents, not connector bytes or all query holds. */
@Internal
public record SnapshotPinPressure(Instant observedAt, List<StateCount> states) {
    public static final int MAX_PER_STATE = 1_000;

    public SnapshotPinPressure {
        Objects.requireNonNull(observedAt, "observedAt");
        states = List.copyOf(states);
        if (states.size() != 3 || states.stream().map(StateCount::state).distinct().count() != states.size()) {
            throw new IllegalArgumentException("An observation must cover each unreleased pin state once");
        }
    }

    /** Count is exact unless truncated; oldest intent is exact when count is nonzero. */
    public record StateCount(SnapshotPinState state, int countLowerBound, boolean truncated,
                             Optional<Instant> oldestCreatedAt) {
        public StateCount {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(oldestCreatedAt, "oldestCreatedAt");
            if (state == SnapshotPinState.RELEASED || countLowerBound < 0 || countLowerBound > MAX_PER_STATE
                    || (countLowerBound > 0) != oldestCreatedAt.isPresent()
                    || (truncated && countLowerBound == 0)) {
                throw new IllegalArgumentException("Invalid bounded pin-state observation");
            }
        }
    }
}
