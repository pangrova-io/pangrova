package org.pangrova.coordinator.index.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NativeIndexRoutingTest {

    @Test
    void routesEveryAiIndexTypeExclusivelyToNativeArtifacts() {
        assertThat(IndexDdlExecutor.requiresNativeArtifact(IndexType.VECTOR)).isTrue();
        assertThat(IndexDdlExecutor.requiresNativeArtifact(IndexType.INVERTED)).isTrue();
        assertThat(IndexDdlExecutor.requiresNativeArtifact(IndexType.BLOOM_FILTER)).isFalse();
    }

    @Test
    void advertisesEveryAlgorithmImplementedByTheNativeBuildOperator() {
        assertThat(DurableIndexBuildService.supports(index(
                IndexType.VECTOR, Map.of("algorithm", "DISK_ANN")))).isTrue();
        assertThat(DurableIndexBuildService.supports(index(
                IndexType.INVERTED, Map.of("algorithm", "BM25")))).isTrue();
        assertThat(DurableIndexBuildService.supports(index(
                IndexType.VECTOR, Map.of("algorithm", "HNSW")))).isTrue();
        assertThat(DurableIndexBuildService.supports(index(
                IndexType.VECTOR, Map.of("algorithm", "IVF_PQ")))).isTrue();
    }

    private static IndexDescriptor index(
            IndexType type, Map<String, String> properties) {
        Instant now = Instant.parse("2026-09-12T00:00:00Z");
        return new IndexDescriptor(
                "index-1", "search_idx", "catalog", "schema", "items",
                List.of("value"), type, properties, IndexState.BUILDING,
                null, 0, "snapshot-1", null, now, now, null);
    }
}
