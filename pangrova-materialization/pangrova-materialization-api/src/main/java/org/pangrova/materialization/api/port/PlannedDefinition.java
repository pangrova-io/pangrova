package org.pangrova.materialization.api.port;

import java.util.Set;
import org.pangrova.materialization.api.domain.Binding;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PartitionAlignment;
import org.pangrova.materialization.api.domain.PolicySnapshot;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.connector.api.materialization.SourceRef;

/**
 * Bound, validated definition produced by a planner adapter.
 *
 * <p>Planning also pins the exact source snapshots the attempt will read, so an
 * attempt reservation never runs against a source set the planner did not
 * observe. A source that cannot be pinned makes planning fail.
 *
 * @param binding normalized SQL and plan fingerprint
 * @param outputSchema output schema
 * @param sourceVector exact source snapshots observed while planning
 * @param dependencies materialization objects consumed by the definition
 * @param policySnapshot source policy fingerprints
 * @param supportedRefreshKinds refresh kinds the shape and sources can support
 * @param partitionAlignment source partition columns exposed by the definition
 */
public record PlannedDefinition(
        Binding binding,
        OutputSchema outputSchema,
        SourceVector sourceVector,
        Set<Dependency> dependencies,
        PolicySnapshot policySnapshot,
        Set<RefreshKind> supportedRefreshKinds,
        PartitionAlignment partitionAlignment) {

    public PlannedDefinition {
        if (binding == null || outputSchema == null || sourceVector == null || policySnapshot == null) {
            throw new IllegalArgumentException(
                    "binding, outputSchema, sourceVector, and policySnapshot must not be null");
        }
        if (dependencies == null || supportedRefreshKinds == null) {
            throw new IllegalArgumentException("dependencies and refresh kinds must not be null");
        }
        if (partitionAlignment == null) {
            throw new IllegalArgumentException("partitionAlignment must not be null");
        }
        dependencies = Set.copyOf(dependencies);
        supportedRefreshKinds = Set.copyOf(supportedRefreshKinds);
        if (supportedRefreshKinds.isEmpty()) {
            throw new IllegalArgumentException("a materializable definition must support at least full refresh");
        }
        if (!supportedRefreshKinds.contains(RefreshKind.Full.INSTANCE)) {
            throw new IllegalArgumentException("every materializable definition must support full refresh");
        }
        if (!policySnapshot.fingerprints().keySet().equals(sourceVector.snapshots().keySet())) {
            throw new IllegalArgumentException(
                    "policy snapshot must contain exactly one entry per pinned source");
        }
    }

    /**
     * Returns the source tables pinned by this definition.
     *
     * @return source set
     */
    public Set<SourceRef> sources() {
        return sourceVector.snapshots().keySet();
    }
}
