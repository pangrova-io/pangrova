package org.pangrova.materialization.usecase;

import java.util.Set;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.InternalNamespace;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.port.DefinitionPlanner;
import org.pangrova.materialization.api.port.PlannedDefinition;
import org.pangrova.materialization.api.port.PlanningRequest;

/** Plans definition revisions and checks the stored source, policy and schema identity at replay. */
final class MaterializationDefinitionPlanning {
    private final DefinitionPlanner planner;

    MaterializationDefinitionPlanning(DefinitionPlanner planner) {
        this.planner = planner;
    }

    PlannedDefinition plan(CreateMaterialization command, long revision) {
        return planner.plan(new PlanningRequest(command.object(), command.kind(),
                command.definitionSql(), command.definer(), command.definerGroups(), revision,
                command.sessionProperties()));
    }

    PlannedDefinition planAndValidate(MaterializationId object, MaterializationKind kind,
            DefinitionRevision definition, org.pangrova.materialization.api.domain.Principal definer,
            java.util.Set<String> definerGroups,
            java.util.Map<String, String> sessionProperties) {
        PlannedDefinition planned = planner.plan(new PlanningRequest(
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
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
