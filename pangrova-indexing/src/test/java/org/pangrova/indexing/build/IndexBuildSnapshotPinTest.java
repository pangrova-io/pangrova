package org.pangrova.indexing.build;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.spi.connector.index.SnapshotPinProvider;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexBuildSnapshotPinTest {

    @Test
    void pinFailurePreventsSuccessfulBuildResult() {
        var provider = new RecordingPinProvider();
        provider.fail = true;
        try (var builder = builder(provider)) {
            assertThatThrownBy(() -> builder.submitBuildTask(descriptor("42"), List.of(), null, null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("pin denied");
            assertThat(provider.pins.get()).isEqualTo(1);
        }
    }

    @Test
    void invalidSnapshotIdentifierPreventsSuccessfulBuildResult() {
        var provider = new RecordingPinProvider();
        try (var builder = builder(provider)) {
            assertThatThrownBy(() -> builder.submitBuildTask(descriptor("bad-id"), List.of(), null, null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(NumberFormatException.class);
            assertThat(provider.pins.get()).isZero();
        }
    }

    @Test
    void successfulPinAllowsCompletedBuild() {
        var provider = new RecordingPinProvider();
        try (var builder = builder(provider)) {
            assertThat(builder.submitBuildTask(descriptor("42"), List.of(), null, null).join()
                    .isFullySuccessful()).isTrue();
            assertThat(provider.pins.get()).isEqualTo(1);
        }
    }

    @Test
    void pinFailurePreventsSourceFileBuild() {
        var provider = new RecordingPinProvider();
        provider.fail = true;
        var context = new IndexBuildContext(new IndexingConfig("test-index", false, null, null),
                new IndexEngineRegistry(List.of()), new IndexStorageManager(),
                new IndexSerializer(null), null, null, provider, null);
        try (var builder = new IndexBuildCoordinator(context, null)) {
            var file = new DataFileInfo("/data/source.parquet", 1, 100, "parquet");
            assertThatThrownBy(() -> builder.submitBuildTask(
                    descriptor("42"), List.of(file), null, null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("pin denied");
        }
    }

    @Test
    void tableWithoutAnySnapshotNeedsNoPin() {
        var provider = new RecordingPinProvider();
        try (var builder = builder(provider)) {
            assertThat(builder.submitBuildTask(descriptor(""), List.of(), null, null).join()
                    .isFullySuccessful()).isTrue();
            assertThat(provider.pins.get()).isZero();
        }
    }

    private static IndexBuildCoordinator builder(SnapshotPinProvider provider) {
        return new IndexBuildCoordinator(new IndexBuildContext(
                new IndexingConfig("test-index", false, null, null),
                null, null, null, null, null, provider, null), null);
    }

    private static IndexDescriptor descriptor(String snapshotId) {
        var now = Instant.now();
        return new IndexDescriptor("index-1", "idx", "cat", "sch", "tbl", List.of("col"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING, null, 0,
                snapshotId, snapshotId, now, now, "tenant-a");
    }

    private static final class RecordingPinProvider implements SnapshotPinProvider {
        private final AtomicInteger pins = new AtomicInteger();
        private boolean fail;

        @Override
        public void pinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
            pins.incrementAndGet();
            if (fail) {
                throw new IllegalStateException("pin denied");
            }
        }

        @Override
        public void unpinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
        }

        @Override
        public List<Long> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }
    }
}
