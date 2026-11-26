package org.pangrova.persistence.indexing.api.metadata;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexPersistenceGuaranteesTest {
    @Test
    void absentDurableOperationsRejectWithoutReportingSuccessOrMissingState() {
        IndexMetadataStore store = storeWithStaleMappings(List.of());
        assertThatThrownBy(() -> store.pollCacheEvents(0, 5)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.insertCacheEvent("drop", "i", "c", "s", "t", Instant.EPOCH))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getBreakerState("i")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.updateBreakerState("i", 1, Instant.EPOCH, null, "OPEN"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.deleteFileMappingsByGeneration("i", 2))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.isIndexGenerationBuildable("i", 2))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.isIndexSnapshotBuildable("i", "snapshot"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.failIndexGeneration("i", 2, Instant.EPOCH, Instant.EPOCH, "lost"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.findIndexBuildRecoveryCandidates())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.abandonIndexGeneration(
                "i", 2, Instant.EPOCH, Instant.EPOCH, Instant.EPOCH, "lost"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.completeIndexBuildCleanup(
                "i", 2, Instant.EPOCH, Instant.EPOCH, org.pangrova.indexing.api.enums.IndexState.READY, 1))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.batchInsertFileStatistics("tenant", "c", "s", "t", List.of()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.deleteFileStatistics("tenant", "c", "s", "t", List.of("data")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getFileStatistics("tenant", "c", "s", "t", "snapshot", List.of(), List.of()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void withholdsMappingsCreatedAtOrAfterTheGarbageCollectionCutoff() {
        Instant cutoff = Instant.parse("2026-01-01T00:00:00Z");
        var expired = mapping("old", cutoff.minusSeconds(1));
        var store = storeWithStaleMappings(List.of(mapping("new", cutoff.plusSeconds(1)),
                expired, mapping("boundary", cutoff)));
        assertThat(store.listStaleFileMappings(1000, cutoff)).containsExactly(expired);
    }

    private static IndexFileMapping mapping(String path, Instant createdAt) {
        return new IndexFileMapping("index", path, "index/" + path, 1, 1, "0".repeat(64),
                1, "snapshot", 0, createdAt, 0);
    }

    private static IndexMetadataStore storeWithStaleMappings(List<IndexFileMapping> mappings) {
        return (IndexMetadataStore) Proxy.newProxyInstance(IndexMetadataStore.class.getClassLoader(),
                new Class<?>[]{IndexMetadataStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("listStaleFileMappings") && method.getParameterCount() == 1) {
                        return mappings;
                    }
                    return InvocationHandler.invokeDefault(proxy, method, args);
                });
    }
}
