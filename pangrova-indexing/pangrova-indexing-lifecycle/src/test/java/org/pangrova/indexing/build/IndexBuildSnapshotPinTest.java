package org.pangrova.indexing.build;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

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
    void connectorRejectionPreventsSuccessfulBuildResult() {
        var provider = new RecordingPinProvider();
        provider.fail = true;
        try (var builder = builder(provider)) {
            assertThatThrownBy(() -> builder.submitBuildTask(descriptor("bad-id"), List.of(), null, null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("pin denied");
            assertThat(provider.snapshot).isEqualTo("bad-id");
        }
    }

    @Test
    void opaqueSnapshotIdentifierIsPassedToTheConnectorWithoutNumericCoercion() {
        var provider = new RecordingPinProvider();
        try (var builder = builder(provider)) {
            assertThat(builder.submitBuildTask(descriptor("snapshot/opaque"), List.of(), null, null).join()
                    .isFullySuccessful()).isTrue();
            assertThat(provider.snapshot).isEqualTo("snapshot/opaque");
            assertThat(provider.owner.indexId()).isEqualTo("index-1");
            assertThat(provider.owner.generation()).isZero();
            assertThat(provider.owner.attemptId()).isNotBlank();
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
    void failedFileBuildReleasesTheExactAttemptPin() {
        var provider = new RecordingPinProvider();
        var context = new IndexBuildContext(new IndexingConfig("test-index", false, null, null),
                new IndexEngineRegistry(List.of()), new IndexStorageManager(),
                new IndexSerializer(null), null, null, provider, null);
        try (var builder = new IndexBuildCoordinator(context, null)) {
            var file = new DataFileInfo("/data/source.parquet", 1, 100, "parquet");
            var result = builder.submitBuildTask(descriptor("42"), List.of(file), null, null).join();

            assertThat(result.isFullySuccessful()).isFalse();
            assertThat(provider.unpins.get()).isOne();
            assertThat(provider.snapshot).isEqualTo("42");
            assertThat(provider.owner.indexId()).isEqualTo("index-1");
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
        private final AtomicInteger unpins = new AtomicInteger();
        private String snapshot;
        private SnapshotPinOwner owner;
        private boolean fail;

        @Override
        public void pinSnapshot(String catalog, String schema, String table, String snapshotToken,
                                SnapshotPinOwner pinOwner) {
            pins.incrementAndGet();
            snapshot = snapshotToken;
            owner = pinOwner;
            if (fail) {
                throw new IllegalStateException("pin denied");
            }
        }

        @Override
        public void unpinSnapshot(String catalog, String schema, String table, String snapshotToken,
                                  SnapshotPinOwner pinOwner) {
            unpins.incrementAndGet();
        }

        @Override
        public void validateSnapshotPin(String catalog, String schema, String table, String snapshotToken,
                                        SnapshotPinOwner pinOwner) {
        }

        @Override
        public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }

        @Override
        public List<SnapshotPinReference> getSnapshotPinsForIndex(
                String catalog, String schema, String table, String indexId) {
            return List.of();
        }
    }
}
