package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Exact immutable index generation and optional source snapshot selected for a query. */
@Experimental
public record IndexFileSelection(int generation, String sourceSnapshot) {

    public IndexFileSelection {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
    }

    /** Returns whether the mapping belongs to this query-pinned selection. */
    public boolean matches(IndexFileMapping mapping) {
        return mapping.generation() == generation;
    }

    /** Returns whether an active manifest satisfies this query snapshot. */
    public boolean matches(IndexGenerationManifest manifest) {
        return matchesPublication(manifest.generation(), manifest.sourceSnapshot());
    }

    public boolean matchesPublication(int publishedGeneration, String publishedSnapshot) {
        return publishedGeneration == generation
                && (sourceSnapshot.isEmpty()
                        || sourceSnapshot.equals(publishedSnapshot));
    }
}
