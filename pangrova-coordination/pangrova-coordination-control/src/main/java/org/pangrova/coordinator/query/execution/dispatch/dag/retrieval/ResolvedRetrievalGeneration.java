package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import org.pangrova.execution.stage.binding.RetrievalGenerationIntent;
import java.util.Objects;
import org.pangrova.protocol.v2.ArtifactBinding;

/** Query-scoped immutable metadata; physical objects remain task-scoped. */
public record ResolvedRetrievalGeneration(
        RetrievalGenerationIntent intent, ArtifactBinding bindingTemplate,
        int sourceObjectCount, long sourceRowCount) {
    public ResolvedRetrievalGeneration {
        Objects.requireNonNull(intent, "intent must not be null");
        Objects.requireNonNull(bindingTemplate,
                "bindingTemplate must not be null");
        if (sourceObjectCount <= 0 || sourceRowCount < 0) {
            throw new IllegalArgumentException("resolved retrieval requires source objects");
        }
        if (bindingTemplate.getSplitObjectsCount() != 0
                || bindingTemplate.hasBindingChecksum()) {
            throw new IllegalArgumentException(
                    "resolved template must not contain task-scoped objects or checksum");
        }
    }
}

