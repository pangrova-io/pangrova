package org.pangrova.coordinator.index.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.index.ddl.IndexBuildServices;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.index.jdbc.JdbcIndexMetadataStore;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class GeneratedValueCommitLogIntegrationTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void failedGeneratedValueProcessorIsRetriedBeforeCommitAcknowledgement()
            throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "generated_value_commit_log");
        JdbcIndexMetadataStore store = new JdbcIndexMetadataStore(dataSource);
        IndexBuildCoordinator buildCoordinator = buildCoordinator(temporaryDirectory);
        MaintenanceLeaderElection leaderElection = new MaintenanceLeaderElection(
                new JdbcDistributedLease(dataSource), "generated-value-test-coordinator");
        IndexCommitHandler handler = new IndexCommitHandler(
                context(store, buildCoordinator), null, leaderElection, 16);
        AtomicInteger attempts = new AtomicInteger();
        handler.registerDurableCommitProcessor(event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("transient generated-value admission failure");
            }
        });
        try {
            handler.onCommit(new CommitEvent(
                    "tenant-a",
                    "lake", "sales", "documents",
                    List.of("s3://warehouse/object-a.parquet"), List.of(),
                    Instant.parse("2026-09-13T00:00:00Z"), "41",
                    CommitEventType.APPEND, null));

            handler.pollCommitLogOnce();
            assertThat(attempts).hasValue(1);
            assertThat(store.pollUnprocessedCommitEvents(16, 1L)).hasSize(1);

            handler.pollCommitLogOnce();
            assertThat(attempts).hasValue(2);
            assertThat(store.pollUnprocessedCommitEvents(16, 1L)).isEmpty();
        } finally {
            handler.close();
            buildCoordinator.close();
        }
    }

    private static IndexBuildCoordinator buildCoordinator(Path storageDirectory) {
        IndexingConfig config = new IndexingConfig(
                storageDirectory.toString(), false, null, null);
        IndexBuildContext buildContext = new IndexBuildContext(
                config, new IndexEngineRegistry(List.of()),
                new IndexStorageManager(),
                new IndexSerializer(TypeSerializerRegistry.createDefault()));
        return new IndexBuildCoordinator(buildContext, null);
    }

    private static IndexCommitContext context(
            JdbcIndexMetadataStore store, IndexBuildCoordinator buildCoordinator) {
        return new IndexCommitContext(
                store, new IndexBuildServices(buildCoordinator, null),
                new ConnectorAccessorResolver(null));
    }

}
