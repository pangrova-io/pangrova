package org.pangrova.persistence.indexing.api.build;

import java.util.UUID;
import org.pangrova.common.api.annotation.Experimental;

/** Exact identity of one immutable scalar object written by a never-reused build attempt. */
@Experimental
public record ScalarArtifactIntent(String indexId, int generation, String attemptOwner,
                                   String dataFilePath, String artifactPath) {

    private static final int INDEX_ID_MAX_LENGTH = 255;
    private static final int FILE_PATH_MAX_LENGTH = 2048;

    public ScalarArtifactIntent {
        requireText(indexId, INDEX_ID_MAX_LENGTH);
        requireText(dataFilePath, FILE_PATH_MAX_LENGTH);
        requireText(artifactPath, FILE_PATH_MAX_LENGTH);
        if (generation < 0 || attemptOwner == null
                || !UUID.fromString(attemptOwner).toString().equals(attemptOwner)) {
            throw new IllegalArgumentException("A canonical scalar artifact owner and generation are required");
        }
    }

    private static void requireText(String value, int maximumLength) {
        if (value == null || value.isBlank() || value.length() > maximumLength) {
            throw new IllegalArgumentException("Invalid scalar artifact identity");
        }
    }
}
