package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.util.Set;

/** Complete source-file coverage for one exact published Native index generation. */
@Experimental
public interface IndexGenerationCoverage {

    /**
     * Reads source-object identities from the durable artifact publication backing
     * the active Native generation. Zero-row source partitions remain covered.
     * Missing, corrupt or incomplete publication evidence must fail closed.
     */
    default Set<String> getNativeIndexSourcePaths(
            IndexDescriptor descriptor, IndexGenerationManifest active) {
        throw new UnsupportedOperationException(
                "Durable Native index generation coverage is unavailable");
    }
}
