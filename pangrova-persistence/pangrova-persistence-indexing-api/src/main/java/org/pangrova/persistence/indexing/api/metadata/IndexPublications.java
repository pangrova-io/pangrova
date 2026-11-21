package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.Optional;
import java.util.List;
import java.util.Map;

/** Atomic activation and lookup of immutable published generations. */
@Experimental
public interface IndexPublications {
    /**
     * Reads mappings and their ACTIVE manifests from one consistent database snapshot.
     * A selection matches its exact generation and, when supplied, the opaque manifest
     * source snapshot. An immutable file may have been copied from an earlier generation.
     * Missing or superseded selections return no mappings. This read grants no retention lease.
     */
    default Map<String, Map<String, IndexFileMapping>> batchGetIndexArtifacts(
            Map<String, IndexFileSelection> selections, List<String> dataFilePaths) {
        throw new UnsupportedOperationException("Consistent published artifact selection is required");
    }

    /** Atomically validates and activates one complete immutable generation. */
    IndexGenerationManifest publishIndexGeneration(IndexPublicationRequest request);

    /** Finds the active immutable generation manifest for an index. */
    Optional<IndexGenerationManifest> findActiveIndexGeneration(String indexId);
}
