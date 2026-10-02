package org.pangrova.metadata.artifact.jdbc;

import java.time.Instant;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;

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
