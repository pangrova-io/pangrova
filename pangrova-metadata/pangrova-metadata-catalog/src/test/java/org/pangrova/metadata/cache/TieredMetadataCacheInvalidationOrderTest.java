package org.pangrova.metadata.cache;

import org.junit.jupiter.api.Test;
import org.pangrova.cache.memory.MemoryCacheStoreFactory;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TieredMetadataCacheInvalidationOrderTest {
    private static final CatalogDeploymentIdentity FIRST = new CatalogDeploymentIdentity("cat", "original", 1);
    private static final TableMetadata OLD = table("old");
    private static final TableMetadata NEW = table("new");

    @Test void invalidationRemovesTheTableFromBothTiersAndPreservesSiblings() {
        var l2 = new MemoryCacheStoreFactory().open(Map.of());
        var writer = new TieredMetadataCache(100, 60, l2);
        writer.putTable("cat", "s", "old", OLD);
        writer.putTable("other", "s", "new", NEW);
        writer.invalidateTable("cat", "s", "old");
        assertThat(writer.getTable("cat", "s", "old")).isEmpty();
        var reader = new TieredMetadataCache(100, 60, l2);
        assertThat(reader.getTable("cat", "s", "old")).isEmpty();
        assertThat(reader.getTable("other", "s", "new")).contains(NEW);
        writer.putTable("cat", "s", "old", OLD);
        writer.invalidateCatalog("cat");
        assertThat(reader.getTable("cat", "s", "old")).isEmpty();
        assertThat(reader.getTable("other", "s", "new")).contains(NEW);
    }

    @Test void fillingAnObsoleteReadCannotOverwriteTheNewGeneration() throws Exception {
        try (var l2 = new MemoryCacheStoreFactory().open(Map.of())) {
            var first = new TieredMetadataCache(100, 60, l2);
            var second = new TieredMetadataCache(100, 60, l2);
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var pool = Executors.newSingleThreadExecutor();
            try {
                var oldRead = pool.submit(() -> first.loadTable(FIRST, "s", "t", () -> {
                    started.countDown();
                    try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                    catch (InterruptedException e) { throw new AssertionError(e); }
                    return OLD;
                }));
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                second.invalidateCatalog("cat");
                assertThat(second.loadTable(FIRST, "s", "t", () -> NEW)).isEqualTo(NEW);
                release.countDown();
                assertThat(oldRead.get(5, TimeUnit.SECONDS)).isEqualTo(OLD);
                assertThat(first.loadTable(FIRST, "s", "t", () -> { throw new AssertionError("stale fill"); }))
                        .isEqualTo(NEW);
            } finally { release.countDown(); pool.shutdownNow(); }
        }
    }

    @Test void sameNamedRecreatedCatalogNeverReadsPreviousDeployment() {
        var cache = new TieredMetadataCache();
        assertThat(cache.loadTable(FIRST, "s", "t", () -> OLD)).isEqualTo(OLD);
        var replacement = new CatalogDeploymentIdentity("cat", "replacement", 1);
        assertThat(cache.loadTable(replacement, "s", "t", () -> NEW)).isEqualTo(NEW);
    }

    private static TableMetadata table(String name) {
        return new TableMetadata("cat", "s", name, List.of(), null, null, null, null, null);
    }
}
