package org.pangrova.materialization.api.port;

import java.util.Map;
import java.util.Optional;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.RefreshPlan;

/**
 * Data-plane execution request for one reserved attempt.
 *
 * <p>The request carries the attempt, its exact definition revision, the
 * narrow write capability, and the resolved refresh plan. It never carries row
 * data and never exposes a coordinator-side row API; the adapter maps it to the
 * canonical v2 task plan executed by workers and native operators.
 *
 * @param attempt reserved refresh attempt
 * @param definition definition revision being executed
 * @param capability internal write capability bound to the attempt
 * @param options connector and strategy options
 * @param plan resolved refresh plan producing the canonical SQL
 */
public record ExecutionRequest(
        RefreshAttempt attempt,
        DefinitionRevision definition,
        MaterializationWriteCapability capability,
        Map<String, String> options,
        Optional<RefreshPlan> plan) {

    public ExecutionRequest {
        if (attempt == null || definition == null || capability == null) {
            throw new IllegalArgumentException("attempt, definition, and capability must not be null");
        }
        if (!attempt.object().equals(definition.object())) {
            throw new IllegalArgumentException("attempt and definition belong to different objects");
        }
        if (attempt.definitionRevision() != definition.revision()) {
            throw new IllegalArgumentException("attempt and definition revisions differ");
        }
        if (!attempt.id().equals(capability.attempt())) {
            throw new IllegalArgumentException("write capability belongs to a different attempt");
        }
        if (!attempt.candidateTable().sameTable(capability.candidateTable())) {
            throw new IllegalArgumentException("write capability is bound to a different candidate table");
        }
        options = options == null ? Map.of() : Map.copyOf(options);
        plan = plan == null ? Optional.empty() : plan;
        plan.ifPresent(resolved -> {
            if (!resolved.kind().equals(attempt.refreshKind())) {
                throw new IllegalArgumentException(
                        "refresh plan kind " + resolved.kind() + " differs from the reserved attempt");
            }
        });
    }
}
