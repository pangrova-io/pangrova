package org.pangrova.coordinator.indexing.lifecycle.gc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.config.GcConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.indexing.storage.LocalFileSystemAccessor;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;
import org.pangrova.persistence.indexing.api.metadata.IndexCleanup.MappedArtifactRetirement;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappedArtifactRetirementIntegrationTest extends ContainerTestBase {
    private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
    @TempDir Path root;

    @Test
    void durableRetirementSurvivesCollectorFailureAndPreventsPhysicalPathReuse() throws Exception {
        verifyRetirement(SharedPostgresContainer.migratedDataSource("mapped_artifact_retirement"), root);
    }

    @Test
    void failedGenerationAndDropUseTheSameDurableRetirementHandoff() throws Exception {
        verifyGenerationAndDropRetirement(
                SharedPostgresContainer.migratedDataSource("generation_artifact_retirement"), root);
    }

    static void verifyGenerationAndDropRetirement(DataSource source, Path root) throws Exception {
        var store = metadata(source);
        var descriptor = descriptor("restart-drop");
        store.registerIndex(descriptor);
        var layout = new IndexStorageLayout(root.toString());
        Path serving = createArtifact(layout, descriptor, 4, "active", "active");
        seed(source, descriptor.indexId(), "active", serving, 4);
        Instant reserved = CREATED.plusSeconds(1);
        assertThat(store.beginIndexGeneration(descriptor.indexId(), 5, "old", reserved,
                descriptor.updatedAt(), descriptor.state(), descriptor.generation())).isTrue();
        var claims = new JdbcIndexBuildClaimRepository(source);
        String oldOwner = UUID.randomUUID().toString();
        assertThat(claims.tryClaim(descriptor.indexId(), 5, oldOwner, reserved)).isTrue();
        Path old = createArtifact(layout, descriptor, 5, "old-source", "old", UUID.fromString(oldOwner));
        store.registerIndexFile(oldOwner, new IndexFileMapping(descriptor.indexId(), "old-source",
                old.toString(), 1, 1, "a".repeat(64), 5, "old", 0, reserved, 0));
        Instant failed = reserved.plusSeconds(1);
        assertThat(store.failIndexGeneration(descriptor.indexId(), 5, reserved, failed, "failed")).isTrue();
        assertThat(claims.release(descriptor.indexId(), 5, oldOwner)).isTrue();

        Instant replacement = failed.plusSeconds(1);
        assertThat(store.beginIndexGeneration(descriptor.indexId(), 5, "new", replacement,
                descriptor.updatedAt(), descriptor.state(), descriptor.generation())).isTrue();
        assertThat(old).exists();
        assertThat(store.pollMappedArtifactRetirements(10)).extracting(MappedArtifactRetirement::artifactPath)
                .containsExactly(old.toString());
        String newOwner = UUID.randomUUID().toString();
        assertThat(claims.tryClaim(descriptor.indexId(), 5, newOwner, replacement)).isTrue();
        Path next = createArtifact(layout, descriptor, 5, "next-source", "next", UUID.fromString(newOwner));
        store.registerIndexFile(newOwner, new IndexFileMapping(descriptor.indexId(), "next-source",
                next.toString(), 1, 1, "b".repeat(64), 5, "new", 0, replacement, 0));
        assertThat(store.completeIndexBuildCleanup(descriptor.indexId(), 5, descriptor.updatedAt(),
                failed, descriptor.state(), descriptor.generation())).isFalse();

        var config = new IndexingConfig(root.toString(), false, null,
                new LifecycleConfig(new GcConfig(1, 0), null, null));
        var storage = new IndexStorageManager();
        try (var gc = new IndexGarbageCollector(new IndexGcContext(store, storage,
                ignored -> new LocalFileSystemAccessor(), config, null), null)) {
            gc.runGc();
            assertThat(old).doesNotExist();
            assertThat(next).exists();
            assertThat(serving).exists();
            assertThat(claims.release(descriptor.indexId(), 5, newOwner)).isTrue();
            store.dropIndex(descriptor.indexId(), replacement.plusSeconds(1));
            assertThatThrownBy(() -> store.purgeIndex(descriptor.indexId()))
                    .hasMessageContaining("remaining mappings");
            assertThat(gc.runGc().purgedCount()).isZero();
            assertThat(store.getIndexFileMappings(descriptor.indexId())).isEmpty();
            assertThat(next).exists();
            assertThat(serving).exists();
            assertThat(store.hasPendingMappedArtifactRetirements(descriptor.indexId())).isTrue();
        }
        try (var failing = new IndexGarbageCollector(new IndexGcContext(metadata(source), storage,
                ignored -> { throw new IllegalStateException("storage unavailable"); }, config, null), null)) {
            assertThat(failing.runGc().purgedCount()).isZero();
            assertThat(store.findIndex(descriptor.indexId())).isPresent();
            assertThat(next).exists();
        }
        try (var recovered = new IndexGarbageCollector(new IndexGcContext(metadata(source), storage,
                ignored -> new LocalFileSystemAccessor(), config, null), null)) {
            assertThat(recovered.runGc().purgedCount()).isOne();
            assertThat(next).doesNotExist();
            assertThat(serving).doesNotExist();
            assertThat(store.findIndex(descriptor.indexId())).isEmpty();

            verifyDropPreservesSharedReference(source, store, root, recovered);
        }
    }

    private static void verifyDropPreservesSharedReference(DataSource source, JdbcIndexMetadataStore store,
            Path root, IndexGarbageCollector gc) throws Exception {
        var left = descriptor("drop-left");
        var right = descriptor("drop-right");
        store.registerIndex(left);
        store.registerIndex(right);
        Path shared = Files.writeString(root.resolve("drop-shared.idx"), "shared");
        seed(source, left.indexId(), "left", shared, 4);
        seed(source, right.indexId(), "right", shared, 4);
        store.dropIndex(left.indexId(), CREATED.plusSeconds(1));
        assertThat(gc.runGc().purgedCount()).isOne();
        assertThat(store.findIndex(left.indexId())).isEmpty();
        assertThat(store.getIndexFileMappings(right.indexId())).extracting(IndexFileMapping::indexFilePath)
                .containsExactly(shared.toString());
        assertThat(shared).exists();
        assertThat(store.pollMappedArtifactRetirements(10)).isEmpty();
    }

    static void verifyRetirement(DataSource source, Path root) throws Exception {
        var store = metadata(source);
        var descriptor = descriptor("retire");
        store.registerIndex(descriptor);
        var layout = new IndexStorageLayout(root.toString());
        Path old = createArtifact(layout, descriptor, 0, "source", "old");
        Path crash = createArtifact(layout, descriptor, 0, "crash-source", "crash");
        Path current = createArtifact(layout, descriptor, 4, "current-source", "current");
        Path legacy = Files.writeString(root.resolve("legacy.idx"), "legacy");
        Path unrelated = Files.writeString(root.resolve("unrelated"), "preserve");
        var original = seed(source, descriptor.indexId(), "source", old, 0);
        var copied = seed(source, descriptor.indexId(), "source", old, 1);
        var crashed = seed(source, descriptor.indexId(), "crash-source", crash, 1);
        var unknown = seed(source, descriptor.indexId(), "legacy-source", legacy, 1);
        seed(source, descriptor.indexId(), "current-source", current, 4);

        var firstRemoval = store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4, List.of(original));
        assertThat(firstRemoval.mappingsRemoved()).isOne();
        assertThat(firstRemoval.pathsToDelete()).isEmpty();
        assertThat(store.pollMappedArtifactRetirements(10)).isEmpty();

        var removal = store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4,
                List.of(copied, crashed, unknown));
        assertThat(removal.mappingsRemoved()).isEqualTo(3);
        assertThat(removal.pathsToDelete()).containsExactlyInAnyOrder(
                old.toString(), crash.toString(), legacy.toString());
        // The metadata transaction committed while no physical delete has occurred.
        assertThat(old).exists();
        assertThat(crash).exists();
        var restarted = metadata(source);
        var first = restarted.pollMappedArtifactRetirements(1).get(0);
        var second = metadata(source).pollMappedArtifactRetirements(1).get(0);
        var third = restarted.pollMappedArtifactRetirements(1).get(0);
        assertThat(List.of(first.artifactPath(), second.artifactPath(), third.artifactPath()))
                .containsExactlyInAnyOrder(old.toString(), crash.toString(), legacy.toString());
        assertThat(restarted.pollMappedArtifactRetirements(1)).containsExactly(first);
        var oldReceipt = receiptFor(List.of(first, second, third), old);
        var crashReceipt = receiptFor(List.of(first, second, third), crash);
        assertThat(oldReceipt.retiredGeneration()).isEqualTo(1);
        assertThat(restarted.beginMappedArtifactReclaim(oldReceipt)).isTrue();

        var config = new IndexingConfig(root.toString(), false, null,
                new LifecycleConfig(new GcConfig(1, 0), null, null));
        var storage = new IndexStorageManager();
        try (var gc = new IndexGarbageCollector(new IndexGcContext(restarted, storage,
                ignored -> { throw new IllegalStateException("storage unavailable"); }, config, null), null)) {
            gc.runGc();
            assertThat(old).exists();
            assertThat(restarted.hasPendingMappedArtifactRetirements(descriptor.indexId())).isTrue();
        }
        // Model success followed by process loss before acknowledgement, on this local fixture only.
        assertThat(restarted.beginMappedArtifactReclaim(crashReceipt)).isTrue();
        Files.deleteIfExists(crash);
        try (var gc = new IndexGarbageCollector(new IndexGcContext(metadata(source), storage,
                ignored -> new LocalFileSystemAccessor(), config, null), null)) {
            gc.runGc();
            assertThat(old).doesNotExist();
            assertThat(crash).doesNotExist();
            assertThat(current).exists();
            assertThat(legacy).exists();
            assertThat(unrelated).exists();
            assertThat(restarted.pollMappedArtifactRetirements(10)).extracting(MappedArtifactRetirement::artifactPath)
                    .containsExactly(legacy.toString());
            restarted.completeMappedArtifactReclaim(oldReceipt);
            assertThat(restarted.beginMappedArtifactReclaim(oldReceipt)).isFalse();

            verifyReuseRejected(source, restarted, descriptor, old);
            restarted.dropIndex(descriptor.indexId(), CREATED.plusSeconds(1));
            assertThatThrownBy(() -> restarted.purgeIndex(descriptor.indexId()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("retirement");
            assertThat(gc.runGc().purgedCount()).isZero();
            assertThat(current).exists();
            assertThat(unrelated).exists();
            verifyGlobalReferences(source, restarted, root, gc);
            verifyCompletedReceiptOutlivesDescriptor(source, restarted, root, gc);
        }
    }

    private static void verifyReuseRejected(DataSource source, JdbcIndexMetadataStore store,
            IndexDescriptor descriptor, Path old) {
        var ready = store.findIndex(descriptor.indexId()).orElseThrow();
        Instant next = ready.updatedAt().plusSeconds(1);
        assertThat(store.beginIndexGeneration(ready.indexId(), 5, "new", next,
                ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        String owner = UUID.randomUUID().toString();
        var claims = new JdbcIndexBuildClaimRepository(source);
        assertThat(claims.tryClaim(ready.indexId(), 5, owner, next)).isTrue();
        var recycled = new IndexFileMapping(ready.indexId(), "recycled", old.toString(),
                1, 1, "a".repeat(64), 5, "new", 0, next, 0);
        assertThatThrownBy(() -> store.registerIndexFile(owner, recycled)).isInstanceOf(MetastoreException.class);
        var intent = new ScalarArtifactIntent(ready.indexId(), 5, owner, "recycled", old.toString());
        assertThatThrownBy(() -> store.beginScalarArtifactWrite(intent)).isInstanceOf(MetastoreException.class);
        assertThat(claims.release(ready.indexId(), 5, owner)).isTrue();
    }

    private static void verifyGlobalReferences(DataSource source, JdbcIndexMetadataStore store, Path root,
            IndexGarbageCollector gc) throws Exception {
        var left = descriptor("left");
        var right = descriptor("right");
        store.registerIndex(left);
        store.registerIndex(right);
        Path shared = Files.writeString(root.resolve("shared-legacy.idx"), "shared");
        var leftMapping = seed(source, left.indexId(), "left-data", shared, 0);
        IndexFileMapping rightMapping;
        var executor = Executors.newSingleThreadExecutor();
        try (var writer = source.getConnection()) {
            writer.setAutoCommit(false);
            rightMapping = seed(writer, right.indexId(), "right-data", shared, 0);
            var entered = new CountDownLatch(1);
            var retirement = executor.submit(() -> {
                entered.countDown();
                return store.removeOrphanMappings(left.indexId(), IndexState.READY, 4, List.of(leftMapping));
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> retirement.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            writer.commit();
            assertThat(retirement.get(5, TimeUnit.SECONDS).pathsToDelete()).isEmpty();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(store.getIndexFileMappings(right.indexId())).extracting(IndexFileMapping::indexFilePath)
                .containsExactly(rightMapping.indexFilePath());
        assertThat(store.removeOrphanMappings(right.indexId(), IndexState.READY, 4, List.of(rightMapping))
                .pathsToDelete()).containsExactly(shared.toString());
        gc.runGc();
        assertThat(shared).exists();
    }

    private static void verifyCompletedReceiptOutlivesDescriptor(DataSource source, JdbcIndexMetadataStore store,
            Path root, IndexGarbageCollector gc) throws Exception {
        var descriptor = descriptor("retired-descriptor");
        store.registerIndex(descriptor);
        Path file = createArtifact(new IndexStorageLayout(root.toString()), descriptor, 0, "source", "retire");
        var mapping = seed(source, descriptor.indexId(), "source", file, 1);
        assertThat(store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4, List.of(mapping))
                .pathsToDelete()).containsExactly(file.toString());
        var receipt = receiptFor(store.pollMappedArtifactRetirements(10), file);
        gc.runGc();
        assertThat(file).doesNotExist();
        store.dropIndex(descriptor.indexId(), CREATED.plusSeconds(1));
        store.purgeIndex(descriptor.indexId());
        assertThat(store.findIndex(descriptor.indexId())).isEmpty();
        store.completeMappedArtifactReclaim(receipt);

        var replacement = descriptor("replacement");
        store.registerIndex(replacement);
        assertThatThrownBy(() -> seed(source, replacement.indexId(), "source", file, 0))
                .isInstanceOf(SQLException.class).hasMessageContaining("Retired index artifact");
    }

    private static MappedArtifactRetirement receiptFor(List<MappedArtifactRetirement> receipts, Path path) {
        return receipts.stream().filter(item -> item.artifactPath().equals(path.toString())).findFirst().orElseThrow();
    }

    private static JdbcIndexMetadataStore metadata(DataSource source) {
        return new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    private static IndexDescriptor descriptor(String id) {
        return new IndexDescriptor(id, id, "lake", "raw", "events", List.of("value"), IndexType.HASH,
                Map.of(), IndexState.READY, null, 4, null, null, CREATED, CREATED, "tenant");
    }

    private static Path createArtifact(IndexStorageLayout layout, IndexDescriptor descriptor,
            int generation, String source, String content) throws Exception {
        return createArtifact(layout, descriptor, generation, source, content, UUID.randomUUID());
    }

    private static Path createArtifact(IndexStorageLayout layout, IndexDescriptor descriptor,
            int generation, String source, String content, UUID owner) throws Exception {
        Path path = Path.of(layout.indexFilePath(descriptor.withGeneration(generation, CREATED), owner, source));
        Files.createDirectories(path.getParent());
        return Files.writeString(path, content);
    }

    private static IndexFileMapping seed(DataSource source, String indexId, String data, Path path,
            int generation) throws Exception {
        try (var connection = source.getConnection()) {
            return seed(connection, indexId, data, path, generation);
        }
    }

    private static IndexFileMapping seed(Connection connection, String indexId, String data, Path path,
            int generation) throws Exception {
        try (var statement = connection.prepareStatement("""
                        INSERT INTO index_file_mappings(index_id,data_file_path,index_file_path,generation,created_at,
                            checksum) VALUES (?,?,?,?,?,?)
                        """)) {
            statement.setString(1, indexId);
            statement.setString(2, data);
            statement.setString(3, path.toString());
            statement.setInt(4, generation);
            statement.setTimestamp(5, Timestamp.from(CREATED));
            statement.setString(6, "f".repeat(64));
            statement.executeUpdate();
        }
        return new IndexFileMapping(indexId, data, path.toString(), 0, 0, "f".repeat(64), generation,
                null, 0, CREATED, 0);
    }
}
