package org.pangrova.persistence.indexing.postgresql.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.metadata.service.TableControlWriteAuthorities;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.persistence.indexing.api.metadata.IndexFileSelection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class JdbcIndexSelectionsTest {
    private static final String SPECIAL = "x'); DROP TABLE index_descriptors; -- ,{}\\\"中文";
    private DataSource dataSource;
    private JdbcIndexMetadataStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("index_selections");
        store = new JdbcIndexMetadataStore(dataSource, TableControlWriteAuthorities.postgres()::requireCurrent);
    }

    @Test
    void descriptorSelectionsBindLiteralValuesAndPreserveStateAndTenantScope() {
        store.registerIndex(descriptor(SPECIAL, "tenant", IndexState.READY));
        store.registerIndex(descriptor("stale", "tenant", IndexState.STALE));
        store.registerIndex(descriptor("building", "tenant", IndexState.BUILDING));
        store.registerIndex(descriptor("foreign", "other", IndexState.READY));
        assertThat(store.findIndexesByIds(List.of(SPECIAL, SPECIAL, "missing"))).containsOnlyKeys(SPECIAL);
        assertThat(store.findIndexesByIds(List.of())).isEmpty();
        assertThat(store.findReadyIndexesForColumns("tenant", "catalog", "schema", "table",
                List.of(SPECIAL), false)).extracting(IndexDescriptor::indexId).containsExactly(SPECIAL);
        assertThat(store.findReadyIndexesForColumns("tenant", "catalog", "schema", "table",
                List.of("missing", SPECIAL, SPECIAL), true)).extracting(IndexDescriptor::indexId)
                .containsExactlyInAnyOrder(SPECIAL, "stale");
        assertThat(store.findReadyIndexesForColumns("tenant", "catalog", "schema", "table",
                List.of(), true)).isEmpty();
        assertThat(store.findIndexesByIds(List.of("building", "foreign"))).hasSize(2);
    }

    @Test
    void mappingsKeepSnapshotAndExactGenerationSelectionWithLiteralPaths() {
        store.registerIndex(descriptor("first", "tenant", IndexState.BUILDING));
        store.registerIndex(descriptor("second", "tenant", IndexState.BUILDING));
        store.registerIndexFile(mapping("first", SPECIAL, 0, "snap'0"));
        store.registerIndexFile(mapping("second", SPECIAL, 0, "snap'0"));
        publish("first", 0);
        publish("second", 0);
        var ready = store.findIndex("first").orElseThrow();
        var stagingAt = ready.updatedAt().plusSeconds(1);
        assertThat(store.beginIndexGeneration(ready.indexId(), 1, "snapshot",
                stagingAt, ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        store.registerIndexFile(mapping("first", SPECIAL, 1, "snap'1"));
        assertThat(store.batchGetIndexArtifacts(
                Map.of("first", new IndexFileSelection(1, "")), List.of(SPECIAL))).isEmpty();
        store.publishIndexGeneration(new IndexPublicationRequest("first", "first", 1, "snapshot",
                "VECTOR", new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64),
                "test-writer"), 1, stagingAt, stagingAt.plusSeconds(1)));
        var artifacts = store.batchGetIndexArtifacts(
                Map.of("first", new IndexFileSelection(1, ""),
                        "second", new IndexFileSelection(0, "")), List.of(SPECIAL, SPECIAL));
        assertThat(store.batchGetIndexArtifacts(
                Map.of("first", new IndexFileSelection(0, "")), List.of(SPECIAL))).isEmpty();
        assertThat(store.batchGetIndexArtifacts(
                Map.of("first", new IndexFileSelection(1, "wrong-snapshot")), List.of(SPECIAL))).isEmpty();
        assertThat(artifacts.get("first").get(SPECIAL).generation()).isEqualTo(1);
        assertThat(artifacts.get("second").get(SPECIAL).generation()).isZero();
        assertThat(store.batchGetIndexFileMappings(List.of("first", "second"), List.of(SPECIAL), "snap'1"))
                .containsOnlyKeys("first");
        assertThat(store.batchGetIndexFileMappings(List.of("first"), List.of(SPECIAL), "absent")).isEmpty();
        assertThat(store.batchGetIndexFileMappings(List.of("second"), List.of(SPECIAL), null))
                .isEqualTo(store.batchGetIndexFileMappings(List.of("second"), List.of(SPECIAL)));
        assertThat(store.batchGetIndexFileMappings(List.of(), List.of(SPECIAL))).isEmpty();
        assertThat(store.batchGetIndexArtifacts(Map.of("first", new IndexFileSelection(0, "")), List.of()))
                .isEmpty();
    }

    @Test
    void batchesCountMarkAndCopyOnlySelectedPathsWithoutCrossIndexOrGenerationDeletion() {
        store.registerIndex(descriptor("first", "tenant", IndexState.BUILDING));
        store.registerIndex(descriptor("second", "tenant", IndexState.BUILDING));
        List<String> paths = IntStream.range(0, 1005).mapToObj(i -> SPECIAL + i).toList();
        store.batchRegisterIndexFiles(paths.stream().map(path -> mapping("first", path, 0, "snapshot")).toList());
        store.registerIndexFile(mapping("second", paths.get(0), 0, "snapshot"));
        assertThat(store.getIndexFileMappingCounts(List.of("first", "first"), paths)).containsEntry("first", 1005);
        assertThat(store.batchGetIndexFileMappings(List.of("first"), paths).get("first")).hasSize(1005);
        assertThat(store.filterUnindexedFiles("first", List.of(paths.get(1004), "missing", "missing")))
                .containsExactly("missing", "missing");
        store.markOrphanedByDataFiles("first", paths.subList(0, 1001));
        assertThat(store.getIndexFileMappings("first").stream().filter(m -> m.lastVerifiedAt() == 0)).hasSize(1001);
        assertThat(store.getIndexFileMappings("second")).allMatch(m -> m.lastVerifiedAt() == 10000);
        var seeded = store.findIndex("first").orElseThrow();
        store.publishIndexGeneration(new IndexPublicationRequest("first", "first", 0,
                seeded.buildSnapshotId(), seeded.indexType().name(),
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                1005, seeded.updatedAt(), seeded.updatedAt().plusSeconds(1)));
        var ready = store.findIndex("first").orElseThrow();
        assertThat(store.beginIndexGeneration(ready.indexId(), 1, "snapshot",
                ready.updatedAt().plusSeconds(1), ready.updatedAt(), ready.state(), ready.generation()))
                .isTrue();
        assertThat(store.copyIndexGenerationMappings("first", 0, 1, paths.subList(0, 1001))).isEqualTo(4);
        assertThat(store.getIndexFileMappings("first").stream().filter(m -> m.generation() == 0)).hasSize(1005);
        assertThat(store.getIndexFileMappings("first").stream().filter(m -> m.generation() == 1))
                .extracting(IndexFileMapping::dataFilePath).containsExactlyInAnyOrderElementsOf(paths.subList(1001, 1005));
        assertThat(store.getIndexFileMappingCounts(List.of("first"), List.of())).isEmpty();
        assertThat(store.filterUnindexedFiles("first", List.of())).isEmpty();
        store.markOrphanedByDataFiles("first", List.of());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preparationFailureFreesArrayAndClosesConnectionWithoutMaskingPrimaryFailure(boolean freeFails) {
        AtomicInteger created = new AtomicInteger();
        AtomicInteger freed = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        DataSource failing = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        Connection actual = (Connection) invoke(dataSource, method, args);
                        return failingConnection(actual, created, freed, closed, freeFails);
                    }
                    return invoke(dataSource, method, args);
                });
        var reader = new JdbcIndexDescriptorReadStore(failing);
        MetastoreException failure = catchThrowableOfType(
                () -> reader.findIndexesByIds(List.of(SPECIAL)), MetastoreException.class);
        assertThat(failure).hasRootCauseMessage("prepare failure");
        assertThat(created).hasValue(1);
        assertThat(freed).hasValue(1);
        assertThat(closed).hasValue(1);
        assertThat(failure.getCause().getSuppressed()).hasSize(freeFails ? 1 : 0);
        if (freeFails) {
            assertThat(failure.getCause().getSuppressed()[0]).hasMessage("free failure");
        }
    }

    private Connection failingConnection(Connection actual, AtomicInteger created,
                                         AtomicInteger freed, AtomicInteger closed, boolean freeFails) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareStatement")) {
                        throw new SQLException("prepare failure");
                    }
                    if (method.getName().equals("close")) {
                        closed.incrementAndGet();
                    }
                    Object result = invoke(actual, method, args);
                    if (method.getName().equals("createArrayOf")) {
                        created.incrementAndGet();
                        return trackedArray((java.sql.Array) result, freed, freeFails);
                    }
                    return result;
                });
    }

    private java.sql.Array trackedArray(java.sql.Array array, AtomicInteger freed, boolean freeFails) {
        return (java.sql.Array) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Array.class},
                (proxy, method, args) -> {
                    Object result = invoke(array, method, args);
                    if (method.getName().equals("free")) {
                        freed.incrementAndGet();
                        if (freeFails) {
                            throw new SQLException("free failure");
                        }
                    }
                    return result;
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private void publish(String indexId, int generation) {
        store.publishIndexGeneration(new IndexPublicationRequest(indexId, indexId, generation, "snapshot", "VECTOR",
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                1, Instant.EPOCH, Instant.EPOCH.plusSeconds(1)));
    }

    private static IndexDescriptor descriptor(String id, String tenant, IndexState state) {
        return new IndexDescriptor(id, "name-" + id, "catalog", "schema", "table", List.of(SPECIAL),
                IndexType.VECTOR, Map.of(), state, null, 0, "snapshot", "snapshot", Instant.EPOCH, Instant.EPOCH, tenant);
    }

    private static IndexFileMapping mapping(String id, String path, int generation, String snapshot) {
        return new IndexFileMapping(id, path, id + "-artifact-" + generation + path, 10, 256, "a".repeat(64),
                generation, snapshot, 0, Instant.EPOCH, 10000);
    }
}
