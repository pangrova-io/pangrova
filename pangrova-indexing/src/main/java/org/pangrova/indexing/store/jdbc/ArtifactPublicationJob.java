package org.pangrova.indexing.store.jdbc;

import java.time.Instant;
import org.pangrova.indexing.api.ArtifactDependencySet;
import org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy;
import org.pangrova.indexing.api.ArtifactExecutionKind;

/** Package-local view shared by type-specific atomic publication adapters. */
interface ArtifactPublicationJob {
    String tenantId();
    String jobId();
    String artifactId();
    String artifactType();
    String generation();
    String sourceId();
    String sourceSnapshot();
    ArtifactExecutionKind executionKind();
    ArtifactDependencySet dependencySet();
    ArtifactEmptyPartitionPolicy emptyPartitionPolicy();
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
