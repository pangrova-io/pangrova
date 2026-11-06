package org.pangrova.persistence.catalog.postgresql.synchronization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncConflictException;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncPendingException;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresMetadataSyncRepositoryTest {
    private static final Instant CHECKPOINT = Instant.parse("2026-01-01T00:00:00.123456789Z");
    private DataSource source;
    private PostgresCatalogDeploymentRepository deployments;
    private PostgresMetadataSyncRepository repository;
    private CatalogDescriptor descriptor;
    private CatalogDeploymentIdentity scope;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        deployments = new PostgresCatalogDeploymentRepository(source);
        descriptor = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(descriptor);
        scope = new CatalogDeploymentIdentity(descriptor.name(), descriptor.incarnationId(), descriptor.revision());
        repository = new PostgresMetadataSyncRepository(source);
    }

    @Test
    void failedHistoryNeverBecomesProgress() {
        var completion = completion(Optional.empty(), Optional.empty(), 1, 1);
        repository.complete(completion);
        assertThat(repository.loadCheckpoint(scope)).isEmpty();
        assertThat(repository.findCompletion(completion.runId())).contains(completion);
    }

    @Test
    void successfulButUnboundedInventoryNeverBecomesProgress() {
        repository.complete(completion(Optional.empty(), Optional.empty(), 10, 0));
        assertThat(repository.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void certifiedEmptyBatchAdvancesAndSurvivesRecreationWithoutPrecisionLoss() {
        var completion = completion(Optional.empty(), Optional.of(CHECKPOINT), 0, 0);
        repository.complete(completion);
        var reopened = new PostgresMetadataSyncRepository(source);
        assertThat(reopened.loadCheckpoint(scope)).contains(CHECKPOINT);
        assertThat(reopened.findCompletion(completion.runId())).contains(completion);
    }

    @Test
    void identicalReplayAfterSubsequentProgressDoesNotRegressIt() {
        var first = completion(Optional.empty(), Optional.of(CHECKPOINT), 2, 0);
        repository.complete(first);
        repository.complete(completion(Optional.of(CHECKPOINT), Optional.of(CHECKPOINT.plusSeconds(1)), 0, 0));
        repository.complete(first);
        assertThat(repository.loadCheckpoint(scope)).contains(CHECKPOINT.plusSeconds(1));
    }

    @Test
    void conflictingRunContentIsRejected() {
        var first = completion(Optional.empty(), Optional.of(CHECKPOINT), 2, 0);
        repository.complete(first);
        var different = new MetadataSyncCompletion(first.runId(), scope, first.expectedCheckpoint(),
                first.nextCheckpoint(), new SyncResult("catalog", "INCREMENTAL", CHECKPOINT, 3, 0, List.of(), 1));
        assertThatThrownBy(() -> repository.complete(different)).isInstanceOf(MetadataSyncConflictException.class);
        assertThat(repository.findCompletion(first.runId())).contains(first);
    }

    @Test
    void staleCheckpointCannotWriteReceipt() {
        repository.complete(completion(Optional.empty(), Optional.of(CHECKPOINT), 1, 0));
        var stale = completion(Optional.empty(), Optional.of(CHECKPOINT.plusSeconds(1)), 1, 0);
        assertThatThrownBy(() -> repository.complete(stale)).isInstanceOf(MetadataSyncConflictException.class);
        assertThat(repository.findCompletion(stale.runId())).isEmpty();
    }

    @Test
    void newIncarnationDoesNotInheritProgressAndRejectsOldCompletion() {
        var first = completion(Optional.empty(), Optional.of(CHECKPOINT), 1, 0);
        repository.complete(first);
        deployments.deleteCatalogDescriptor(descriptor.name());
        var replacement = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(replacement);
        var newScope = new CatalogDeploymentIdentity(replacement.name(), replacement.incarnationId(), replacement.revision());
        assertThat(repository.loadCheckpoint(newScope)).isEmpty();
        assertThatThrownBy(() -> repository.complete(completion(Optional.of(CHECKPOINT),
                Optional.of(CHECKPOINT.plusSeconds(1)), 1, 0))).isInstanceOf(MetadataSyncConflictException.class);
        repository.complete(first); // Confirmation/replay of an already completed old run remains valid.
        assertThat(repository.findCompletion(first.runId())).contains(first);
    }

    @Test
    void revisionReplacementRejectsOldCompletion() {
        deployments.saveCatalogDescriptor(new CatalogDescriptor(descriptor.name(), descriptor.type(), descriptor.provider(),
                descriptor.state(), Map.of("endpoint", "changed"), "", descriptor.allowedTenants(),
                descriptor.incarnationId(), descriptor.revision() + 1));
        var stale = completion(Optional.empty(), Optional.of(CHECKPOINT), 1, 0);
        assertThatThrownBy(() -> repository.complete(stale)).isInstanceOf(MetadataSyncConflictException.class);
        assertThat(repository.findCompletion(stale.runId())).isEmpty();
    }

    @Test
    void progressFailureRollsBackInsertedReceipt() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_sync_progress ADD CONSTRAINT fail_progress CHECK (revision < 0)");
        }
        var completion = completion(Optional.empty(), Optional.of(CHECKPOINT), 1, 0);
        assertThatThrownBy(() -> repository.complete(completion)).isInstanceOf(MetadataSyncPendingException.class).hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThat(repository.findCompletion(completion.runId())).isEmpty();
        assertThat(repository.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void concurrentInitialCompletionsHaveExactlyOneWinner() throws Exception {
        var first = completion(Optional.empty(), Optional.of(CHECKPOINT), 1, 0);
        var second = completion(Optional.empty(), Optional.of(CHECKPOINT.plusSeconds(1)), 1, 0);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var a = executor.submit(() -> attempt(start, first));
            var b = executor.submit(() -> attempt(start, second));
            start.countDown();
            assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(List.of(repository.findCompletion(first.runId()), repository.findCompletion(second.runId()))
                    .stream().filter(Optional::isPresent).count()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean attempt(CountDownLatch start, MetadataSyncCompletion completion) throws InterruptedException {
        start.await();
        try {
            repository.complete(completion);
            return true;
        } catch (MetadataSyncConflictException | MetadataSyncPendingException expected) {
            return false;
        }
    }

    private MetadataSyncCompletion completion(Optional<Instant> expected, Optional<Instant> next, int applied, int errors) {
        return new MetadataSyncCompletion(UUID.randomUUID(), scope, expected, next,
                new SyncResult("catalog", "INCREMENTAL", CHECKPOINT.plusSeconds(10), applied, errors,
                        errors == 0 ? List.of() : List.of("projection rejected"), 2));
    }
}
