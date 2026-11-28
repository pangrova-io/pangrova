package org.pangrova.coordinator.indexing.lifecycle.gc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.config.BuildConfig;
import org.pangrova.indexing.config.GcConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.config.RuntimeConfig;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.indexing.storage.LocalFileSystemAccessor;
import org.pangrova.indexing.strategies.hash.provider.HashStrategyProvider;
import org.pangrova.indexing.strategies.hash.serializer.HashSerializer;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScalarArtifactReclamationTest extends ContainerTestBase {
    @TempDir Path root;

    @Test
    void mappingCallbackFailureLeavesDurableLocalArtifactForGc() throws Exception {
        verifyReclamation(SharedPostgresContainer.migratedDataSource("scalar_reclaim"), root);
    }

    static void verifyReclamation(DataSource source, Path root) throws Exception {
        var store = new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
        var claims = new JdbcIndexBuildClaimRepository(source);
        var config = new IndexingConfig(root.toString(), true,
                new RuntimeConfig(new BuildConfig(1, 1, 1), null, null),
                new LifecycleConfig(new GcConfig(1, 0), null, null));
        var serializer = new IndexSerializer(TypeSerializerRegistry.builder()
                .register(IndexType.HASH, new HashSerializer()).build());
        var fs = new LocalFileSystemAccessor();
        Instant start = Instant.now().minusSeconds(10);
        var descriptor = new IndexDescriptor("scalar-reclaim", "idx", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, null, null, start, start, "tenant");
        store.registerIndex(descriptor);
        ColumnValueReader reader = new ColumnValueReader() {
            @Override public Iterator<List<Comparable<?>>> readColumns(String file, List<String> columns) {
                return List.<List<Comparable<?>>>of(List.of(1L), List.of(2L)).iterator();
            }
            @Override public void close() {}
        };
        var storage = new IndexStorageManager();
        try (var coordinator = new IndexBuildCoordinator(new IndexBuildContext(config,
                new IndexEngineRegistry(List.of(new HashStrategyProvider())), storage, serializer,
                claims, (id, generation) -> true, null, null), null, store)) {
            assertThatThrownBy(() -> coordinator.submitBuildTask(descriptor,
                    List.of(new DataFileInfo("source", 2, 10, "test")), fs, reader, batch -> {
                        throw new IllegalStateException("mapping unavailable");
                    }).get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
        var intents = store.listReclaimableScalarArtifacts(Instant.now().plusSeconds(30), 10);
        assertThat(intents).hasSize(1);
        Path artifact = Path.of(intents.get(0).artifactPath());
        assertThat(serializer.deserialize(Files.readAllBytes(artifact))).isPresent();
        assertThat(store.getIndexFileMappings(descriptor.indexId())).isEmpty();
        Path unrelated = Files.writeString(root.resolve("unrelated"), "preserve");
        try (var gc = new IndexGarbageCollector(new IndexGcContext(store, storage,
                ignored -> { throw new IllegalStateException("storage unavailable"); }, config, null), null)) {
            assertThat(gc.runGc().orphanedCount()).isZero();
            assertThat(store.hasScalarArtifactIntents(descriptor.indexId())).isTrue();
            assertThat(artifact).exists();
        }
        try (var gc = new IndexGarbageCollector(new IndexGcContext(store, storage,
                ignored -> fs, config, null), null)) {
            assertThat(gc.runGc().orphanedCount()).isEqualTo(1);
            assertThat(artifact).doesNotExist();
            assertThat(unrelated).exists();
            assertThat(store.hasScalarArtifactIntents(descriptor.indexId())).isFalse();

            UUID uncertainOwner = UUID.randomUUID();
            var uncertainPath = new IndexStorageLayout(root.toString()).indexFilePath(
                    descriptor, uncertainOwner, "uncertain");
            var intent = new ScalarArtifactIntent(descriptor.indexId(), 0, uncertainOwner.toString(),
                    "uncertain", uncertainPath);
            assertThat(claims.tryClaim(descriptor.indexId(), 0, uncertainOwner.toString(), start)).isTrue();
            store.beginScalarArtifactWrite(intent);
            Path uncertain = Path.of(uncertainPath);
            Files.createDirectories(uncertain.getParent());
            Files.writeString(uncertain, "uncertain write");
            assertThat(claims.release(descriptor.indexId(), 0, uncertainOwner.toString())).isTrue();
            store.dropIndex(descriptor.indexId(), start.plusSeconds(1));
            assertThat(gc.runGc().purgedCount()).isZero();
            assertThat(store.findIndex(descriptor.indexId())).isPresent();
            assertThat(uncertain).exists();
        }
    }
}
