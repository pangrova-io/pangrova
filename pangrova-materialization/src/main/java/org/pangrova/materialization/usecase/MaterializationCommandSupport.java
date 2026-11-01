package org.pangrova.materialization.usecase;

import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Shared command-protocol helpers of the materialization kernel: idempotent
 * receipts, CAS schedule mutations, published-generation replay, and
 * plan-to-definition stability validation.
 */
final class MaterializationCommandSupport {

    private MaterializationCommandSupport() {
    }

    /** Rejects a command receipt that belongs to a different materialization. */
    static void requireReceiptScope(CommandRecord receipt, MaterializationId object) {
        String marker = "\"object\":\"" + object.canonicalName() + "\"";
        if (!receipt.resultReference().contains(marker)) {
            throw MaterializationException.requestRejected(
                    "command id " + receipt.commandId().value()
                            + " was already used for another materialization");
        }
    }

    /** Replays a completed command against its published generation. */
    static MaterializationOutcome replay(KernelPorts ports, MaterializationId object) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        Generation published = ports.repository().findPublishedGeneration(object)
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "replayed command has no published generation: " + object.canonicalName()));
        return new MaterializationOutcome(header, published, false);
    }

    /** Applies a schedule mutation with bounded compare-and-set retries. */
    static Schedule casSchedule(KernelPorts ports, MaterializationId object,
            UnaryOperator<Schedule> mutation) {
        for (int attempt = 0; attempt < 3; attempt++) {
            Schedule current = ports.repository().findSchedule(object)
                    .orElseThrow(() -> MaterializationException.requestRejected(
                            "materialization " + object.canonicalName() + " has no schedule"));
            Schedule updated = mutation.apply(current);
            if (updated.version() == current.version()) {
                return current;
            }
            try {
                ports.repository().updateSchedule(updated, current.version());
                return updated;
            } catch (MaterializationException conflict) {
                if (conflict.getErrorCode()
                        != PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION) {
                    throw conflict;
                }
            }
        }
        throw MaterializationException.concurrentPublication(
                "could not update the schedule of " + object.canonicalName());
    }

    /** Suspends an active schedule, leaving already stopped schedules untouched. */
    static void suspendSchedule(KernelPorts ports, MaterializationId object) {
        if (ports.repository().findSchedule(object)
                .filter(schedule -> schedule.state() == ScheduleState.ACTIVE)
                .isEmpty()) {
            return;
        }
        casSchedule(ports, object, schedule -> schedule.state() == ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }

    /**
     * Re-plans a definition and verifies that its declared sources, output
     * schema, and dependency edges still match the persisted revision.
     */
    static PlannedDefinition planAndValidate(KernelPorts ports, MaterializationId object,
            MaterializationKind kind, DefinitionRevision definition, Principal definer,
            Set<String> definerGroups, Map<String, String> sessionProperties) {
        PlannedDefinition planned = ports.planner().plan(new PlanningRequest(
                object, kind, definition.sql(), definer, definerGroups, definition.revision(),
                sessionProperties));
        boolean generatedSources = hasGeneratedSources(planned.sources())
                || hasGeneratedSources(definition.sources());
        if (!declaredSources(planned.sources()).equals(declaredSources(definition.sources()))) {
            throw MaterializationException.invalidDefinition(
                    "definition sources changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.outputSchema().equals(definition.outputSchema())) {
            throw MaterializationException.invalidDefinition(
                    "definition output schema changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.dependencies().equals(definition.dependencies())) {
            throw MaterializationException.invalidDefinition(
                    "definition dependencies changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        // An upstream generation table is replaced on every publication, so the
        // physical plan necessarily changes when a definition reads another
        // materialization. The declared sources, output schema, and dependency
        // edges above are the stable semantics; only definitions without
        // generated sources bind a plan and policy snapshot for their lifetime.
        if (!generatedSources) {
            if (!planned.binding().planFingerprint().equals(definition.binding().planFingerprint())) {
                throw MaterializationException.invalidDefinition(
                        "definition plan changed; the object must be recreated or altered: "
                                + object.canonicalName());
            }
            if (!planned.policySnapshot().fingerprints()
                    .equals(definition.policySnapshot().fingerprints())) {
                throw MaterializationException.policyDrift(
                        "source policy changed for " + object.canonicalName());
            }
        }
        return planned;
    }

    private static boolean hasGeneratedSources(Set<SourceRef> sources) {
        return sources.stream()
                .anyMatch(source -> InternalNamespace.SCHEMA_NAME.equals(source.schema()));
    }

    private static Set<SourceRef> declaredSources(Set<SourceRef> sources) {
        return sources.stream()
                .filter(source -> !InternalNamespace.SCHEMA_NAME.equals(source.schema()))
                .collect(Collectors.toUnmodifiableSet());
    }
}
