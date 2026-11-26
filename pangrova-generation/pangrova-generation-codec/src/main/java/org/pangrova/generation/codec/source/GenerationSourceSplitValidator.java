package org.pangrova.generation.codec.source;

import org.pangrova.protocol.v2.DataSplit;

/** Verifies exact immutable source identities for publication reads. */
public final class GenerationSourceSplitValidator {
    private GenerationSourceSplitValidator() {}

    /** Rejects missing or unknown canonical identity fields before resolving artifacts. */
    public static void validateSplit(DataSplit split) {
        if (!split.getUnknownFields().asMap().isEmpty()
                || split.getSplitId().isBlank()
                || split.getSourceId().isBlank()
                || split.getSourceSnapshot().isBlank()
                || split.getDataObjectId().isBlank()) {
            throw new IllegalArgumentException(
                    "artifact resolution requires a canonical split identity");
        }
    }

}
