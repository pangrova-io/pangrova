package org.pangrova.generation.api.page;

import org.pangrova.generation.api.enums.GenerationPageKind;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Stable work-page identity and exact source coverage. */
public record GenerationPage(
        String jobId,
        String pageId,
        int stageId,
        int partitionId,
        int pageOrdinal,
        GenerationPageKind kind,
        List<GenerationSplit> splits,
        List<String> dependencyPageIds,
        int maxAttempts) {

    public GenerationPage {
        requireIdentity(jobId, pageId);
        requireCoordinates(stageId, partitionId, pageOrdinal, maxAttempts);
        kind = Objects.requireNonNull(kind, "kind must not be null");
        splits = List.copyOf(splits);
        dependencyPageIds = List.copyOf(dependencyPageIds);
        requireUniqueInputs(splits, dependencyPageIds);
        requireInputsForKind(kind, splits, dependencyPageIds);
    }

    private static void requireIdentity(String jobId, String pageId) {
        if (jobId == null || jobId.isBlank() || pageId == null || pageId.isBlank()) {
            throw new IllegalArgumentException("jobId and pageId must not be blank");
        }
    }

    private static void requireCoordinates(
            int stageId, int partitionId, int pageOrdinal, int maxAttempts) {
        if (stageId < 0 || partitionId < 0 || pageOrdinal < 0 || maxAttempts <= 0) {
            throw new IllegalArgumentException("invalid artifact work-page coordinates");
        }
    }

    private static void requireUniqueInputs(
            List<GenerationSplit> splits, List<String> dependencyPageIds) {
        if (new HashSet<>(sourceObjectIds(splits)).size() != splits.size()
                || new HashSet<>(dependencyPageIds).size() != dependencyPageIds.size()
                || dependencyPageIds.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new IllegalArgumentException("artifact page inputs must have unique identities");
        }
    }

    private static void requireInputsForKind(
            GenerationPageKind kind,
            List<GenerationSplit> splits,
            List<String> dependencyPageIds) {
        if (kind == GenerationPageKind.RELATIONAL_COMPUTE
                || kind == GenerationPageKind.RELATIONAL_PARTITION) {
            if (!splits.isEmpty() && !dependencyPageIds.isEmpty()) {
                throw new IllegalArgumentException("relational exchange consumers cannot also enumerate source splits");
            }
            return;
        }
        boolean sourcePage = kind == GenerationPageKind.SOURCE_MAP;
        if (sourcePage != (splits.size() == 1 && dependencyPageIds.isEmpty())
                || (!sourcePage && (!splits.isEmpty() || dependencyPageIds.isEmpty()))) {
            throw new IllegalArgumentException(
                    "source pages require splits; reduction pages require dependencies");
        }
    }

    public List<String> sourceObjectIds() {
        return sourceObjectIds(splits);
    }

    private static List<String> sourceObjectIds(List<GenerationSplit> splits) {
        return splits.stream().map(GenerationSplit::dataObjectId).toList();
    }
}
