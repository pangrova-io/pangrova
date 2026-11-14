package org.pangrova.indexing.pruning;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableSplit;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.evaluation.IndexEvaluationService;
import org.pangrova.indexing.storage.LocalFileSystemAccessor;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.indexing.api.evaluation.IndexEvaluatedSplit;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.pangrova.protocol.index.IndexEvalHintProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.security.api.identity.UserIdentity;

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
    private JdbcIndexBuildClaimRepository claims;
    private javax.sql.DataSource dataSource;
    private JdbcGrantStore grants;
    private DynamicRbacAccessControl accessControl;
    private final UserIdentity user = new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
    private ConnectorSplit split;
    private Path dataFile;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("index_eval_authority");
        metadata = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        claims = new JdbcIndexBuildClaimRepository(dataSource);
        var roles = new JdbcRoleStore(dataSource);
        grants = new JdbcGrantStore(dataSource);
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
    void onlyTheSelectedPublishedGenerationReachesTheWorkerSplit() throws Exception {
        String path = dataFile.toString();
        String indexId = "split-index";
        var table = new QualifiedTableName("lake", "public", "items");
        GenerationAuthorityFixtures.provision(dataSource, "split-tenant", "builder",
                DeferredAuthorizationScope.raw(table, Set.of("id"), table));
        metadata.registerIndexWithBuildAuthority(new IndexDescriptor(indexId, "split_idx", "lake", "public", "items",
                List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING, null, 0,
                "snap-0", "snap-0", Instant.EPOCH, Instant.EPOCH, "split-tenant"), "builder");
        assertThat(claims.tryClaim(indexId, 0, "builder", Instant.now())).isTrue();
        metadata.registerIndexFile("builder", new IndexFileMapping(indexId, path, "published-0", 1, 12,
                "a".repeat(64), 0, "object-snapshot", 0, Instant.EPOCH, 0));
        var format = new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer");
        var predicate = new EqualTo("id", 7L);
        var first = new IndexHint(indexId, IndexType.BLOOM_FILTER, 0, predicate);
        var next = new IndexHint(indexId, IndexType.BLOOM_FILTER, 1, predicate);
        var splits = List.of(split);
        try (var filter = filter(10_000)) {
            assertThat(filter.filterSplits(splits, List.of(first), "lake", null, "snap-0")).isSameAs(splits);
            metadata.publishIndexGeneration(new IndexPublicationRequest(indexId, indexId, 0, "snap-0",
                    "BLOOM_FILTER", format, 1, Instant.EPOCH, Instant.EPOCH.plusSeconds(1)));
            var ready = metadata.findIndex(indexId).orElseThrow();
            var stagedAt = ready.updatedAt().plusSeconds(1);
            assertThat(metadata.beginIndexGeneration(indexId, 1, "snap-1", stagedAt, ready.updatedAt(),
                    ready.state(), ready.generation())).isTrue();
            assertThat(claims.tryClaim(indexId, 1, "builder", Instant.now())).isTrue();
            metadata.registerIndexFile("builder", new IndexFileMapping(indexId, path, "published-1", 1, 12,
                    "c".repeat(64), 1, "snap-1", 0, Instant.EPOCH, 0));
            assertThat(filter.filterSplits(splits, List.of(next), "lake", null, "snap-1")).isSameAs(splits);
            var active = (IndexEvaluatedSplit) filter.filterSplits(splits, List.of(first), "lake",
                    null, "snap-0").get(0);
            var wireHints = IndexEvalHintProtoCodec.extractHints(
                    DataSplit.parseFrom(IndexEvalHintProtoCodec.serializeWithHints(active)));
            assertThat(wireHints).singleElement().satisfies(hint -> {
                assertThat(hint.indexFilePath()).isEqualTo("published-0");
                assertThat(hint.indexSizeBytes()).isEqualTo(12L);
                assertThat(hint.checksum()).isEqualTo("a".repeat(64));
                assertThat(hint.generation()).isZero();
            });
            metadata.publishIndexGeneration(new IndexPublicationRequest(indexId, indexId, 1, "snap-1",
                    "BLOOM_FILTER", format, 1, stagedAt, stagedAt.plusSeconds(1)));
            assertThat(filter.filterSplits(splits, List.of(first), "lake", null, "snap-0")).isSameAs(splits);
            assertThat(filter.filterSplits(splits, List.of(next), "lake", null, "snap-0")).isSameAs(splits);
            var published = (IndexEvaluatedSplit) filter.filterSplits(splits, List.of(next), "lake",
                    null, "snap-1").get(0);
            assertThat(published.evalResult().hintsForFile(path)).singleElement().satisfies(hint -> {
                assertThat(hint.indexFilePath()).isEqualTo("published-1");
                assertThat(hint.generation()).isEqualTo(1);
            });
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
                new IndexStorageManager(), new IndexSerializer(TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class))));
    }
}
