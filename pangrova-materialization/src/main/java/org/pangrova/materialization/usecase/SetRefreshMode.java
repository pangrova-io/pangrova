package org.pangrova.materialization.usecase;

import java.util.Optional;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.StrategyId;

/**
 * Command that changes the refresh mode and follows it with a full publication.
 *
 * <p>The configuration CAS and the full refresh are separate durable steps: if
 * the refresh fails, the new mode is recorded but the published head is
 * unchanged, so a retry recomputes from the last committed generation.
 *
 * @param object target object
 * @param requester principal issuing the command
 * @param mode new refresh mode
 * @param strategy pinned incremental strategy; empty means automatic
 * @param commandId idempotency key
 */
public record SetRefreshMode(
        MaterializationId object,
        Principal requester,
        RefreshMode mode,
        Optional<StrategyId> strategy,
        CommandId commandId) {

    public SetRefreshMode {
        if (object == null || requester == null || mode == null || commandId == null) {
            throw new IllegalArgumentException(
                    "object, requester, refresh mode, and command id are required");
        }
        strategy = strategy == null ? Optional.empty() : strategy;
        if (mode == RefreshMode.FULL && strategy.isPresent()) {
            throw new IllegalArgumentException(
                    "FULL refresh mode cannot carry an incremental strategy");
        }
    }
}
