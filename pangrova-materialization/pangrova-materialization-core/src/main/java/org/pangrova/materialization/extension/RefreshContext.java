package org.pangrova.materialization.extension;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.PartitionAlignment;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.ChangeSetPlan;
import org.pangrova.connector.spi.materialization.SourceRef;

/**
 * Input for refresh strategy validation and planning.
 *
 * @param definition current definition revision
 * @param baseGeneration published base generation for incremental kinds
 * @param sourceVector exact observed source snapshots
 * @param candidateTable reserved candidate table
 * @param changeSets connector change-set plans, empty for full refresh
 * @param changeProof verified change proof, when an incremental strategy is used
 * @param partitionAlignment source partition columns exposed by the definition
 */
public record RefreshContext(
        DefinitionRevision definition,
        Optional<Generation> baseGeneration,
        SourceVector sourceVector,
        PhysicalTableRef candidateTable,
        Map<SourceRef, ChangeSetPlan> changeSets,
        Optional<ChangeProof> changeProof,
        PartitionAlignment partitionAlignment) {

    public RefreshContext {
        if (definition == null || sourceVector == null || candidateTable == null) {
            throw new IllegalArgumentException("definition, sourceVector, and candidateTable must not be null");
        }
        baseGeneration = RequireExtension.notNull(baseGeneration, "baseGeneration");
        changeSets = changeSets == null ? Map.of() : Map.copyOf(changeSets);
        changeProof = RequireExtension.notNull(changeProof, "changeProof");
        partitionAlignment = partitionAlignment == null
                ? PartitionAlignment.NONE : partitionAlignment;
        if (!declared(sourceVector.snapshots().keySet())
                .equals(declared(definition.sources()))) {
            throw new IllegalArgumentException(
                    "source vector must contain exactly the definition sources");
        }
        if (!changeSets.keySet().equals(changeProof.map(proof -> Set.of(proof.source()))
                .orElse(Set.of()))) {
            throw new IllegalArgumentException("change proofs must cover exactly the planned change sets");
        }
        if (baseGeneration.isPresent()
                && !baseGeneration.get().object().equals(definition.object())) {
            throw new IllegalArgumentException("base generation belongs to a different object");
        }
    }

    // An upstream materialization is republished under a new physical table, so
    // only the declared (non-generated) sources are stable definition identity.
    private static Set<SourceRef> declared(Set<SourceRef> sources) {
        return sources.stream()
                .filter(source -> !InternalNamespace.SCHEMA_NAME.equals(source.schema()))
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Creates a full-refresh context without change sets. */
    public RefreshContext(DefinitionRevision definition, Optional<Generation> baseGeneration,
            SourceVector sourceVector, PhysicalTableRef candidateTable) {
        this(definition, baseGeneration, sourceVector, candidateTable, Map.of(), Optional.empty());
    }

    /** Creates a context without partition alignment. */
    public RefreshContext(DefinitionRevision definition, Optional<Generation> baseGeneration,
            SourceVector sourceVector, PhysicalTableRef candidateTable,
            Map<SourceRef, ChangeSetPlan> changeSets, Optional<ChangeProof> changeProof) {
        this(definition, baseGeneration, sourceVector, candidateTable, changeSets, changeProof,
                PartitionAlignment.NONE);
    }
}
