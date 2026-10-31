package org.pangrova.metadata.generation.jdbc;

import java.time.Instant;
import org.pangrova.generation.api.GenerationDependencySet;
import org.pangrova.generation.api.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.GenerationExecutionKind;

/** Package-local view shared by type-specific atomic publication adapters. */
interface GenerationPublicationJob {
    String tenantId();
    String jobId();
    String artifactId();
    String artifactType();
    String generation();
    String sourceId();
    String sourceSnapshot();
    GenerationExecutionKind executionKind();
    GenerationDependencySet dependencySet();
    GenerationEmptyPartitionPolicy emptyPartitionPolicy();
    String formatId();
    int formatMajor();
    int formatMinor();
    String schemaSha256();
    String writerBuildId();
    String encryptionKeyRef();
    int expectedPages();
    int completedPages();
    long publicationFence();
    Instant publicationExpectedUpdatedAt();
}
