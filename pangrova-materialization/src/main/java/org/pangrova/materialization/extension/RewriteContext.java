package org.pangrova.materialization.extension;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Observed facts a rewrite guard evaluates.
 *
 * @param view materialized view candidate
 * @param requester query principal
 * @param querySources sources read by the query
 * @param queryHasPolicies whether row filters or column masks apply to the query
 * @param timeTravel whether the query uses time travel or an explicit snapshot
 * @param nondeterministic whether the query contains a non-deterministic expression
 * @param requesterHasSelect whether the requester may read the materialized view
 * @param generationPublishedAt publication instant of the candidate generation
 * @param maxStaleness maximum accepted staleness; empty when the view is manual-only
 * @param queryStart query planning instant
 */
public record RewriteContext(
        MaterializationId view,
        Principal requester,
        Set<SourceRef> querySources,
        boolean queryHasPolicies,
        boolean timeTravel,
        boolean nondeterministic,
        boolean requesterHasSelect,
        Instant generationPublishedAt,
        Optional<Duration> maxStaleness,
        Instant queryStart) {

    public RewriteContext {
        if (view == null || requester == null || querySources == null || generationPublishedAt == null
                || queryStart == null) {
            throw new IllegalArgumentException("rewrite identity and timestamps must not be null");
        }
        querySources = Set.copyOf(querySources);
        maxStaleness = RequireExtension.notNull(maxStaleness, "maxStaleness");
    }
}
