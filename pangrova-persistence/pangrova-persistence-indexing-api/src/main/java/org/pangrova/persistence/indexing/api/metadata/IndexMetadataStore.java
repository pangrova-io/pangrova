package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Complete repository used to assemble indexing operations backed by the same database. */
@Experimental
public interface IndexMetadataStore extends IndexDescriptors, IndexBuildGenerations, IndexPublications,
        IndexFileMappings, IndexGenerationCoverage, IndexCleanup, IndexCommitLog, IndexCacheEvents,
        IndexBreakerStates, IndexFileStatistics, IndexSchemaEvolutionStore, ScalarSnapshotPins {}
