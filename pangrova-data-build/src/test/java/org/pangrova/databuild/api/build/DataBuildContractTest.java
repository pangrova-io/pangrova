package org.pangrova.databuild.api.build;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBuildContractTest {

    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    @Test
    void rejectsNonPositiveFormatMajorBeforePersistence() {
        assertThatThrownBy(() -> jobWithFormatMajor(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("format major must be positive");
    }

    @Test
    void rejectsMissingGovernedPrincipalBeforeDispatchPersistence() {
        assertThatThrownBy(() -> DataBuildDispatchSpecFactory.create(
                "job-1", org.pangrova.databuild.assembly.DataBuildStageGraphFactory.forArtifactPages(
                        "plan".getBytes(StandardCharsets.UTF_8), "binding".getBytes(StandardCharsets.UTF_8), java.util.List.of(), java.util.List.of()),
                "tenant-a", "",
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new DataBuildDispatchSpec.ResourceGrant(1, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal must not be blank");
    }

    private static DataBuildJob jobWithFormatMajor(int formatMajor) {
        return new DataBuildJob(
                "tenant-a", "job-1", "artifact-1", DataBuildOutputTypeIds.VECTOR_INDEX,
                "generation-1", "catalog.schema.table", "snapshot-1",
                DataBuildExecutionKind.BUILD,
                DataBuildDependencySet.sourceSnapshot(
                        "catalog.schema.table", "snapshot-1"),
                DataBuildEmptyPartitionPolicy.FORBID,
                "s3://bucket/tenants/tenant-a/artifacts/artifact-1/generation-1",
                "pangrova.diskann.segment", formatMajor, 0,
                "a".repeat(64), "native-build", "", "request-1",
                1, 0, DataBuildJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
