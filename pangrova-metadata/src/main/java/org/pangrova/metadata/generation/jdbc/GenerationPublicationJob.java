package org.pangrova.metadata.generation.jdbc;

import java.time.Instant;
import org.pangrova.spi.generation.GenerationDependencySet;
import org.pangrova.spi.generation.GenerationEmptyPartitionPolicy;
import org.pangrova.spi.generation.GenerationExecutionKind;

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
