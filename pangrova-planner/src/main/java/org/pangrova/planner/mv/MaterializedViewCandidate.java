package org.pangrova.planner.mv;

import java.time.Duration;
import java.time.Instant;

/**
 * Holds a matched MV candidate during rewrite evaluation.
 *
 * @param definition the MV definition
 * @param staleness  time elapsed since last refresh
 */
public record MaterializedViewCandidate(
        MaterializedViewDefinition definition,
        Duration staleness) {

    /**
     * Compute staleness from the MV's last refresh snapshot to now.
     */
    public static MaterializedViewCandidate of(MaterializedViewDefinition mv, Instant now) {
        Duration age = mv.lastRefreshSnapshot()
                .map(snap -> Duration.between(snap, now))
                .orElse(Duration.ofDays(365));
        return new MaterializedViewCandidate(mv, age);
    }
}
