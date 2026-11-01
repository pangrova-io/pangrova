package org.pangrova.coordinator.indexing.filter;

import org.pangrova.indexing.assembly.serde.DefaultTypeSerializers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableSplit;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.registry.IndexEngineRegistry;
import org.pangrova.indexing.eval.IndexEvaluationService;
import org.pangrova.indexing.fs.LocalFileSystemAccessor;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.indexing.store.jdbc.JdbcIndexMetadataStore;
import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.empty.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.connector.index.predicate.EqualTo;
import org.pangrova.spi.security.UserIdentity;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL RBAC, filesystem reads and Future boundaries; no index/provider substitutes. */
@Tag("integration")
class IndexEvaluationAuthorityIntegrationTest {

    @TempDir Path directory;
    private JdbcIndexMetadataStore metadata;
    private JdbcGrantStore grants;
    private DynamicRbacAccessControl accessControl;
    private final UserIdentity user = new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
    private ConnectorSplit split;
    private Path dataFile;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("index_eval_authority");
        metadata = new JdbcIndexMetadataStore(dataSource);
        var roles = new JdbcRoleStore(dataSource);
        grants = new JdbcGrantStore(dataSource);
        roles.initSchema();
        grants.initSchema();
        assertThat(roles.createRole(Role.of("reader"))).isTrue();
        assertThat(roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"))).isTrue();
        accessControl = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
        dataFile = directory.resolve("metadata.json");
        Files.writeString(dataFile, "{\"snapshots\":[]}");
        split = new IcebergSystemTableSplit("lake.public.items",
                IcebergSystemTableHandle.Kind.SNAPSHOTS, dataFile.toString(), Files.size(dataFile));
    }

    @Test
    void actualGrantedReadCompletesAndRevokedReadAbortsAtFutureBoundary() throws Exception {
        grantSelect();
        try (var filter = filter(10_000)) {
            CompletableFuture<ConnectorSplit> allowed = authorizedRead();
            assertThat(filter.awaitFuture(allowed, split, 0)).isSameAs(split);
            assertThat(allowed).isCompletedWithValue(split);
            assertThat(grants.revokeGrants("reader", Privilege.SELECT, "lake", "public", "items"))
                    .isEqualTo(1);
            CompletableFuture<ConnectorSplit> denied = authorizedRead();
            Throwable original = denied.handle((value, failure) -> {
                assertThat(value).isNull();
                return failure instanceof CompletionException ? failure.getCause() : failure;
            }).get(10, TimeUnit.SECONDS);
            assertThat(original).isInstanceOf(AuthorizationDeniedException.class);
            assertThatThrownBy(() -> filter.awaitFuture(denied, split, 0)).isSameAs(original);
            assertThat(denied).isCompletedExceptionally();
        }
    }

    @Test
    void deniedSuffixAbortsCollectionWithoutReturningAnAlreadyCompletedPrefix() throws Exception {
        grantSelect();
        CompletableFuture<ConnectorSplit> allowed = authorizedRead();
        assertThat(allowed.get(10, TimeUnit.SECONDS)).isSameAs(split);
        assertThat(grants.revokeGrants("reader", Privilege.SELECT, "lake", "public", "items"))
                .isEqualTo(1);
        CompletableFuture<ConnectorSplit> denied = authorizedRead();
        CompletableFuture<ConnectorSplit> pending = new CompletableFuture<>();
        try (var filter = filter(10_000)) {
            assertThatThrownBy(() -> filter.collectL1Results(List.of(split, split, split),
                    List.of(allowed, denied, pending)))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThat(allowed).isCompletedWithValue(split);
            assertThat(denied).isCompletedExceptionally();
            assertThat(pending).isCancelled();
        } finally {
            pending.cancel(true);
        }
    }

    @Test
    void actualCancellationCannotBecomeACompletedSplit() {
        CompletableFuture<ConnectorSplit> pending = new CompletableFuture<>();
        assertThat(pending.cancel(true)).isTrue();
        try (var filter = filter(10_000)) {
            assertThatThrownBy(() -> filter.awaitFuture(pending, split, 0))
                    .isInstanceOf(CancellationException.class);
            assertThat(pending).isCancelled();
        }
    }

    @Test
    void ownerInterruptionAbortsTheWaitAndPreservesItsFlag() throws Exception {
        CompletableFuture<ConnectorSplit> pending = new CompletableFuture<>();
        var entered = new CountDownLatch(1);
        var outcome = new CompletableFuture<Throwable>();
        var flag = new CompletableFuture<Boolean>();
        try (var filter = filter(10_000)) {
            Thread owner = new Thread(() -> {
                entered.countDown();
                try {
                    filter.awaitFuture(pending, split, 0);
                    outcome.completeExceptionally(new AssertionError("Interrupted owner returned a split"));
                } catch (Throwable failure) {
                    outcome.complete(failure);
                    flag.complete(Thread.currentThread().isInterrupted());
                }
            }, "index-eval-interrupted-owner");
            owner.start();
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                owner.interrupt();
                assertThat(outcome.get(10, TimeUnit.SECONDS)).isInstanceOf(CancellationException.class);
                assertThat(flag.get(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                owner.interrupt();
                owner.join(10_000);
                pending.cancel(true);
            }
            assertThat(owner.isAlive()).isFalse();
        }
    }

    @Test
    void ordinaryFilesystemFailureKeepsTheSameSplitAndPlan() {
        CompletableFuture<ConnectorSplit> missing = CompletableFuture.supplyAsync(() -> {
            try (var input = new LocalFileSystemAccessor().openInputStream(
                    directory.resolve("absent.data").toString())) {
                input.readAllBytes();
                return split;
            } catch (java.io.IOException failure) {
                throw new UncheckedIOException(failure);
            }
        });
        try (var filter = filter(10_000)) {
            assertThat(filter.awaitFuture(missing, split, 0)).isSameAs(split);
            assertThat(missing).isCompletedExceptionally();
        }
    }

    @Test
    void optionalIndexTimeoutKeepsSplitAndCancelsOnlyItsFuture() {
        CompletableFuture<ConnectorSplit> pending = new CompletableFuture<>();
        try (var filter = filter(0)) {
            assertThat(filter.awaitFuture(pending, split, 0)).isSameAs(split);
            assertThat(pending).isCancelled();
        }
    }

    @Test
    void sqlWithoutIndexHintsRetainsTheOriginalSplits() {
        List<ConnectorSplit> splits = List.of(split);
        try (var filter = filter(10_000)) {
            assertThat(filter.filterSplits(splits, List.of(), "lake", null)).isSameAs(splits);
        }
    }

    @Test
    void serviceResolverUsesRealPersistedAuthorityAndDoesNotConvertDenialToUnknown() {
        IndexDescriptor descriptor = storedDescriptor();
        var service = service();
        var fs = new LocalFileSystemAccessor();
        String indexFile = directory.resolve("not_built.index").toString();
        assertThatThrownBy(() -> service.aggregateResults(List.of(descriptor), dataFile.toString(),
                new EqualTo("id", 1L), fs, indexId -> {
                    checkSelect();
                    return indexFile;
                })).isInstanceOf(AuthorizationDeniedException.class);
        grantSelect();
        assertThat(service.aggregateResults(List.of(descriptor), dataFile.toString(),
                new EqualTo("id", 1L), fs, indexId -> {
                    checkSelect();
                    return indexFile;
                })).isEqualTo(IndexEvalResult.UNKNOWN);
        assertThat(grants.revokeGrants("reader", Privilege.SELECT, "lake", "public", "items"))
                .isEqualTo(1);
        assertThatThrownBy(() -> service.aggregateResults(List.of(descriptor), dataFile.toString(),
                new EqualTo("id", 1L), fs, indexId -> {
                    checkSelect();
                    return indexFile;
                })).isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void actualMissingAndCorruptOptionalIndexFilesRemainUnknown() throws Exception {
        IndexDescriptor descriptor = storedDescriptor();
        var service = service();
        var fs = new LocalFileSystemAccessor();
        assertThat(service.evaluateFile(descriptor, dataFile.toString(),
                directory.resolve("absent.index").toString(), new EqualTo("id", 1L), fs))
                .isEqualTo(IndexEvalResult.UNKNOWN);
        Path corrupt = directory.resolve("corrupt.index");
        Files.writeString(corrupt, "invalid index envelope");
        assertThat(service.aggregateResults(List.of(descriptor), dataFile.toString(),
                new EqualTo("id", 1L), fs, indexId -> corrupt.toString()))
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void serviceDoesNotClearAnInterruptedOwnersFlagOrReturnUnknown() {
        IndexDescriptor descriptor = storedDescriptor();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> service().evaluateFile(descriptor, dataFile.toString(),
                    directory.resolve("absent.index").toString(), new EqualTo("id", 1L),
                    new LocalFileSystemAccessor())).isInstanceOf(CancellationException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private IndexSplitFilter filter(long timeoutMillis) {
        return new IndexSplitFilter(metadata, null, new IndexEvalConfig(null, timeoutMillis, 0));
    }

    private CompletableFuture<ConnectorSplit> authorizedRead() {
        return CompletableFuture.supplyAsync(() -> {
            checkSelect();
            try (var input = new LocalFileSystemAccessor().openInputStream(dataFile.toString())) {
                assertThat(new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                        .isEqualTo("{\"snapshots\":[]}");
                return split;
            } catch (java.io.IOException failure) {
                throw new UncheckedIOException(failure);
            }
        });
    }

    private void grantSelect() {
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE",
                "lake", "public", "items", null));
    }

    private void checkSelect() {
        accessControl.checkCanSelectFromTable(user, "lake", "public", "items");
    }

    private IndexDescriptor storedDescriptor() {
        var descriptor = new IndexDescriptor("authority-index", "items_id", "lake", "public", "items",
                List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING, null, 0,
                null, null, Instant.now(), Instant.now(), "default");
        metadata.registerIndex(descriptor);
        IndexDescriptor stored = metadata.findIndexesByIds(List.of(descriptor.indexId())).get(descriptor.indexId());
        assertThat(stored).isNotNull();
        assertThat(stored.indexType()).isEqualTo(IndexType.BLOOM_FILTER);
        return stored;
    }

    private static IndexEvaluationService service() {
        return new IndexEvaluationService(new IndexEngineRegistry(), new IndexCache(1_048_576),
                new IndexStorageManager(), new IndexSerializer(DefaultTypeSerializers.createRegistry()));
    }
}
