package org.pangrova.materialization.port;

import java.util.Map;
import java.util.Set;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.Principal;

/**
 * Parses and binds a definition SQL into the materialable shape.
 *
 * <p>The adapter uses the production planner; the kernel receives only this
 * DTO and therefore never depends on Calcite or planner internals.
 *
 * @param object object being planned
 * @param kind object kind
 * @param definitionSql definition SQL
 * @param definer principal the definition runs as
 * @param definerGroups identity groups bound to the definer
 * @param definitionRevision revision being planned
 * @param sessionProperties planning session properties
 */
public record PlanningRequest(
        MaterializationId object,
        MaterializationKind kind,
        String definitionSql,
        Principal definer,
        Set<String> definerGroups,
        long definitionRevision,
        Map<String, String> sessionProperties) {

    /** Creates a request whose definer carries no group identity. */
    public PlanningRequest(MaterializationId object, MaterializationKind kind, String definitionSql,
            Principal definer, long definitionRevision, Map<String, String> sessionProperties) {
        this(object, kind, definitionSql, definer, Set.of(), definitionRevision, sessionProperties);
    }

    public PlanningRequest {
        if (object == null || kind == null || definer == null) {
            throw new IllegalArgumentException("object, kind, and definer must not be null");
        }
        if (definitionSql == null || definitionSql.isBlank()) {
            throw new IllegalArgumentException("definitionSql must not be blank");
        }
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        definerGroups = definerGroups == null ? Set.of() : Set.copyOf(definerGroups);
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
    }
}
