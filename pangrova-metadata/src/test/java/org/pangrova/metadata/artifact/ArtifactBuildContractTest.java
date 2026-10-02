package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactBuildContractTest {

    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    @Test
    void rejectsNonPositiveFormatMajorBeforePersistence() {
        assertThatThrownBy(() -> jobWithFormatMajor(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("format major must be positive");
    }

    @Test
    void rejectsMissingGovernedPrincipalBeforeDispatchPersistence() {
        assertThatThrownBy(() -> ArtifactBuildDispatchSpec.of(
                "job-1", org.pangrova.metadata.artifact.build.ArtifactStageGraph.forArtifactPages(
                        "plan".getBytes(StandardCharsets.UTF_8), "binding".getBytes(StandardCharsets.UTF_8), java.util.List.of(), java.util.List.of()),
                "tenant-a", "",
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new ArtifactBuildDispatchSpec.ResourceGrant(1, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal must not be blank");
    }

    private static ArtifactBuildJob jobWithFormatMajor(int formatMajor) {
        return new ArtifactBuildJob(
                "tenant-a", "job-1", "artifact-1", ArtifactTypeIds.VECTOR_INDEX,
                "generation-1", "catalog.schema.table", "snapshot-1",
                ArtifactExecutionKind.BUILD,
                ArtifactDependencySet.sourceSnapshot(
                        "catalog.schema.table", "snapshot-1"),
                ArtifactEmptyPartitionPolicy.FORBID,
                "s3://bucket/tenants/tenant-a/artifacts/artifact-1/generation-1",
                "pangrova.diskann.segment", formatMajor, 0,
                "a".repeat(64), "native-build", "", "request-1",
                1, 0, ArtifactBuildJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
