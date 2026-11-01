package org.pangrova.generation.api.job;

import org.pangrova.generation.assembly.GenerationDispatchSpecFactory;

import org.pangrova.generation.api.GenerationDependencySet;
import org.pangrova.generation.api.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.GenerationExecutionKind;
import org.pangrova.generation.api.GenerationOutputTypeIds;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationContractTest {

    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    @Test
    void rejectsNonPositiveFormatMajorBeforePersistence() {
        assertThatThrownBy(() -> jobWithFormatMajor(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("format major must be positive");
    }

    @Test
    void rejectsMissingGovernedPrincipalBeforeDispatchPersistence() {
        assertThatThrownBy(() -> GenerationDispatchSpecFactory.create(
                "job-1", org.pangrova.generation.assembly.GenerationStageGraphFactory.forArtifactPages(
                        "plan".getBytes(StandardCharsets.UTF_8), "binding".getBytes(StandardCharsets.UTF_8), java.util.List.of(), java.util.List.of()),
                "tenant-a", "",
                org.pangrova.spi.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new GenerationDispatchSpec.ResourceGrant(1, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal must not be blank");
    }

    private static GenerationJob jobWithFormatMajor(int formatMajor) {
        return new GenerationJob(
                "tenant-a", "job-1", "artifact-1", GenerationOutputTypeIds.VECTOR_INDEX,
                "generation-1", "catalog.schema.table", "snapshot-1",
                GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.table", "snapshot-1"),
                GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/tenants/tenant-a/artifacts/artifact-1/generation-1",
                "pangrova.diskann.segment", formatMajor, 0,
                "a".repeat(64), "native-build", "", "request-1",
                1, 0, GenerationJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
