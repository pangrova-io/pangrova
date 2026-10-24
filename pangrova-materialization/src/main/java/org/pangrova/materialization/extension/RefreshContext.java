package org.pangrova.materialization.extension;

import java.util.Map;
import java.util.Optional;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Input for refresh strategy validation and planning.
 *
 * @param definition current definition revision
 * @param baseGeneration published base generation for incremental kinds
 * @param sourceVector exact observed source snapshots
 * @param candidateTable reserved candidate table
 * @param changeSets connector change-set plans, empty for full refresh
 * @param changeProof verified change proof, when an incremental strategy is used
 */
public record RefreshContext(
        DefinitionRevision definition,
        Optional<Generation> baseGeneration,
        SourceVector sourceVector,
        PhysicalTableRef candidateTable,
        Map<SourceRef, ChangeSetPlan> changeSets,
        Optional<ChangeProof> changeProof) {

    public RefreshContext {
        if (definition == null || sourceVector == null || candidateTable == null) {
            throw new IllegalArgumentException("definition, sourceVector, and candidateTable must not be null");
        }
        baseGeneration = RequireExtension.notNull(baseGeneration, "baseGeneration");
        changeSets = changeSets == null ? Map.of() : Map.copyOf(changeSets);
        changeProof = RequireExtension.notNull(changeProof, "changeProof");
        if (!sourceVector.snapshots().keySet().equals(definition.sources())) {
            throw new IllegalArgumentException(
                    "source vector must contain exactly the definition sources");
        }
        if (!changeSets.keySet().equals(changeProof.map(proof -> java.util.Set.of(proof.source()))
                .orElse(java.util.Set.of()))) {
            throw new IllegalArgumentException("change proofs must cover exactly the planned change sets");
        }
        if (baseGeneration.isPresent()
                && !baseGeneration.get().object().equals(definition.object())) {
            throw new IllegalArgumentException("base generation belongs to a different object");
        }
    }

    /** Creates a full-refresh context without change sets. */
    public RefreshContext(DefinitionRevision definition, Optional<Generation> baseGeneration,
            SourceVector sourceVector, PhysicalTableRef candidateTable) {
        this(definition, baseGeneration, sourceVector, candidateTable, Map.of(), Optional.empty());
    }
}
