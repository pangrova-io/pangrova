package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.pangrova.protocol.databuild.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.common.api.annotation.Internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact partition budgets for pure source-artifact readers, bounded by their immutable templates. */
@Internal
public final class DataBuildPartitionResources {
    private DataBuildPartitionResources() {}

    /** Applies only after canonical task-local object expansion and checksum verification. */
    public static List<OperatorBinding> bind(List<OperatorBinding> bindings) {
        return bindings.stream().map(DataBuildPartitionResources::bindOperator).toList();
    }

    private static OperatorBinding bindOperator(OperatorBinding binding) {
        var limits = binding.getResources();
        if (binding.getArtifactsCount() == 0 || !isPureReader(limits)) {
            return binding;
        }
        long rows = 0;
        long bytes = 0;
        for (ArtifactBinding artifact : binding.getArtifactsList()) {
            PublishedGenerationBindingChecksum.verify(artifact);
            var totals = sourceTotals(artifact);
            rows = Math.addExact(rows, totals.rows());
            bytes = Math.addExact(bytes, totals.bytes());
        }
        if (rows > limits.getFillKeys() || bytes > limits.getObjectStoreReadBytes()) {
            throw new IllegalStateException("Artifact partition resources exceed their immutable template");
        }
        return binding.toBuilder().setResources(limits.toBuilder()
                .setFillKeys(rows).setObjectStoreReadBytes(bytes)).build();
    }

    private static boolean isPureReader(OperatorResourceRequirement resources) {
        return resources.getFillKeys() > 0 && resources.getObjectStoreReadBytes() > 0
                && resources.toBuilder().clearFillKeys().clearObjectStoreReadBytes().build()
                        .equals(OperatorResourceRequirement.getDefaultInstance());
    }

    private static Totals sourceTotals(ArtifactBinding artifact) {
        if (artifact.getSplitObjectsCount() == 0) {
            throw new IllegalStateException("Artifact reader requires materialized partition objects");
        }
        Map<String, Long> primaryRows = new HashMap<>();
        Set<String> objects = new HashSet<>();
        long bytes = 0;
        for (ArtifactObject object : artifact.getSplitObjectsList()) {
            validateObject(object, objects);
            bytes = Math.addExact(bytes, object.getSizeBytes());
            if (object.getOrdinal() == 0
                    && primaryRows.put(object.getSourceObjectId(), object.getRowCount()) != null) {
                throw new IllegalStateException("Artifact partition repeats a primary source object");
            }
        }
        for (ArtifactObject object : artifact.getSplitObjectsList()) {
            Long rows = primaryRows.get(object.getSourceObjectId());
            if (rows == null || rows.longValue() != object.getRowCount()) {
                throw new IllegalStateException("Artifact source roles lack one exact primary row-count proof");
            }
        }
        long rows = primaryRows.values().stream().reduce(0L, Math::addExact);
        return new Totals(rows, bytes);
    }

    private static void validateObject(ArtifactObject object, Set<String> objects) {
        if (object.getScope() != ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT
                || object.getSourceObjectId().isBlank() || object.getObjectId().isBlank()
                || object.getRowCount() < 0 || object.getSizeBytes() < 0
                || !objects.add(object.getObjectId())) {
            throw new IllegalStateException("Artifact partition has invalid or repeated source object evidence");
        }
    }

    private record Totals(long rows, long bytes) {}
}
