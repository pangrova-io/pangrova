package org.pangrova.materialization.usecase;

import java.util.Optional;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.extension.RefreshStrategy;

/**
 * Resolved execution decision for one attempt.
 *
 * @param kind resolved refresh kind
 * @param base base generation, present exactly for incremental kinds
 * @param strategy strategy that plans and verifies the attempt
 */
record RefreshResolution(RefreshKind kind, Optional<Generation> base, RefreshStrategy strategy) {

    RefreshResolution {
        if (kind == null || base == null || strategy == null) {
            throw new IllegalArgumentException("refresh resolution must be complete");
        }
        if (kind.isIncremental() != base.isPresent()) {
            throw new IllegalArgumentException(
                    "only incremental refreshes resolve a base generation");
        }
    }
}
