package org.pangrova.materialization.api.port;

import java.time.Instant;
import java.util.Set;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.connector.api.materialization.SourceRef;

/**
 * Read-only projection of a published materialization, consumed by the planner bridge.
 *
 * @param object object identity
 * @param kind object kind
 * @param generation published generation
 * @param physicalTable physical generation table
 * @param definitionRevision published definition revision
 * @param definitionSql definition SQL
 * @param outputSchema output schema
 * @param sources source tables
 * @param refreshKind refresh kind that produced the generation
 * @param sourceVector exact source snapshots
 * @param publishedAt publication instant
 */
public record PublishedMaterialization(
        MaterializationId object,
        MaterializationKind kind,
        GenerationId generation,
        PhysicalTableRef physicalTable,
        long definitionRevision,
        String definitionSql,
        OutputSchema outputSchema,
        Set<SourceRef> sources,
        RefreshKind refreshKind,
        SourceVector sourceVector,
        Instant publishedAt) {

    public PublishedMaterialization {
        if (object == null || kind == null || generation == null || physicalTable == null) {
            throw new IllegalArgumentException("object, kind, generation, and physicalTable must not be null");
        }
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        if (definitionSql == null || definitionSql.isBlank()) {
            throw new IllegalArgumentException("definitionSql must not be blank");
        }
        if (outputSchema == null || sources == null || refreshKind == null || sourceVector == null) {
            throw new IllegalArgumentException("schema, sources, refresh kind, and source vector are required");
        }
        sources = Set.copyOf(sources);
        if (publishedAt == null) {
            throw new IllegalArgumentException("publishedAt must not be null");
        }
    }
}
