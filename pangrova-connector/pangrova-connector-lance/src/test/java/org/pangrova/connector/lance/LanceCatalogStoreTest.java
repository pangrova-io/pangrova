package org.pangrova.connector.lance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.protocol.source.SourceSnapshotBindings;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.connector.api.metadata.PreparedSourcePublication;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL catalog CAS, exact SDK candidates, retained identity and concurrent acceptance tests. */
class LanceCatalogStoreTest extends ContainerTestBase {
    @TempDir Path warehouse;
    private LanceManagedSourceFixture fixture;
    private LanceCatalogStore store;
    private String catalog;

    @BeforeEach void setUp() {
        catalog = "catalog_" + UUID.randomUUID();
        fixture = new LanceManagedSourceFixture(catalog, warehouse);
        store = fixture.store();
        store.createSchema(catalog, "tenant-a", "db");
    }

    @Test
    void persistsEmptySchemaWithoutLeakingItToAnotherTenant() {
        String schema = "empty_" + UUID.randomUUID();
        store.createSchema(catalog, "tenant-a", schema);
        assertThat(store.listSchemas(catalog, "tenant-a")).contains(schema);
        assertThat(store.listSchemas(catalog, "tenant-b")).doesNotContain(schema);
        var restarted = new LanceManagedSourceFixture(catalog, warehouse).store();
        assertThat(restarted.listSchemas(catalog, "tenant-a")).contains(schema);
        assertThatThrownBy(() -> store.createSchema(catalog, "tenant-a", " ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void persistentDirectoryIsTenantIsolatedAndPinnedHandleKeepsOldVersion() throws Exception {
        var prepared = prepare("isolated", "isolated", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1, 2);
        var table = prepared.allocation().output().table();
        assertThat(store.load(table.catalog(), "tenant-b", table.table())).isNull();
        prepared.accept();
        var restarted = new LanceManagedSourceFixture(catalog, warehouse).store();
        var visible = restarted.load(table.catalog(), table.tenantId(), table.table());
        assertThat(visible.version()).isEqualTo(1);
        assertThat(visible.rowCount()).isEqualTo(2);
        assertThat(table.version()).isZero();
    }

    @Test
    void sameCommitReplayIsIdempotentAndChangedManifestIsRejected() throws Exception {
        var prepared = prepare("replay", "replay", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1, 2, 3);
        assertThat(prepared.pages()).hasSize(1);
        var publication = fixture.registerObjects(prepared.allocation().executionId(), prepared.pages().get(0));
        var accepted = fixture.acceptRegistered(prepared.allocation().executionId(), publication);
        assertThat(fixture.acceptRegistered(prepared.allocation().executionId(), publication)).isEqualTo(accepted);
        var table = prepared.allocation().output().table();
        assertThat(store.load(table.catalog(), table.tenantId(), table.table()).revision()).isEqualTo(2);
        byte[] changed = accepted.getManifestSha256().getDigest().toByteArray(); changed[0] ^= 1;
        var changedReference = accepted.toBuilder().setManifestSha256(Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256).setDigest(com.google.protobuf.ByteString.copyFrom(changed))).build();
        var conflicting = new PreparedSourcePublication(SourceSnapshotBindings.decode(changedReference, fixture.catalog()),
                publication.rows(), publication.objectsCount(), publication.objectsSha256(), publication.inheritedMembership());
        assertThatThrownBy(() -> fixture.acceptRegistered(prepared.allocation().executionId(), conflicting))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("replay differs");
        assertThat(fixture.reference(table.datasetUri(), 0)).isEqualTo(accepted);
    }

    @Test
    void historicalPublicationRetainsExactSchemaRowsAndNativeDigestAcrossRestart() throws Exception {
        var prepared = prepare("history", "history-first", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1, 2);
        var initial = prepared.allocation().output().table();
        var firstRef = prepared.accept();
        byte[] firstDigest = firstRef.getManifestSha256().getDigest().toByteArray();
        var first = store.load(initial.catalog(), initial.tenantId(), initial.table());
        var secondPrepared = prepare("history", "history-second", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 3, 4, 5);
        secondPrepared.accept();
        var current = store.load(initial.catalog(), initial.tenantId(), initial.table());
        var selector = (LanceTableHandle) current.withTimeTravelSnapshot("1");
        assertThat(selector).isNotSameAs(current);
        var restarted = new LanceManagedSourceFixture(catalog, warehouse).store();
        var historical = restarted.loadSnapshot(selector);
        assertThat(historical.table().version()).isEqualTo(1);
        assertThat(historical.table().rowCount()).isEqualTo(2);
        assertThat(historical.table().columns()).containsExactlyElementsOf(initial.columns());
        assertThat(historical.manifestDigest()).containsExactly(firstDigest);
        assertThat(current.rowCount()).isEqualTo(5);
        byte originalByte = firstDigest[0]; firstDigest[0] ^= 1;
        assertThat(historical.manifestDigest()[0]).isEqualTo(originalByte);
        assertThat(first.version()).isEqualTo(1);
        assertThatThrownBy(() -> restarted.loadSnapshot((LanceTableHandle) current.withTimeTravelSnapshot("3")))
                .hasMessageContaining("Exact accepted source membership is absent");
        assertThatThrownBy(() -> current.withTimeTravelSnapshot("0")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> current.withTimeTravelSnapshot("-1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LanceSnapshotResolver(() -> restarted, Optional.empty(), catalog, warehouse.toString())
                .resolve(LanceSplitManagerTest.session("tenant-a"), selector)).hasMessageContaining("requires Native control");
        assertThatThrownBy(() -> new LanceSnapshotResolver(() -> restarted, Optional.empty(), catalog, warehouse.toString())
                .resolve(LanceSplitManagerTest.session("tenant-b"), selector)).isInstanceOf(SecurityException.class);
    }

    @Test
    void concurrentPublicationsFromOneBaseHaveExactlyOneWinner() throws Exception {
        var left = prepare("concurrent", "left", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var right = prepare("concurrent", "right", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 2);
        var leftPublication = fixture.registerObjects(left.allocation().executionId(), left.pages().get(0));
        var rightPublication = fixture.registerObjects(right.allocation().executionId(), right.pages().get(0));
        var executor = ManagedExecutorFactory.newFixedThreadPool("lance-catalog-cas-test", 2);
        AtomicInteger winners = new AtomicInteger(); AtomicInteger conflicts = new AtomicInteger();
        try {
            var first = executor.submit(() -> publish(left.allocation().executionId(), leftPublication, winners, conflicts));
            var second = executor.submit(() -> publish(right.allocation().executionId(), rightPublication, winners, conflicts));
            first.get(30, TimeUnit.SECONDS); second.get(30, TimeUnit.SECONDS);
            assertThat(winners).hasValue(1); assertThat(conflicts).hasValue(1);
            var table = left.allocation().output().table();
            assertThat(store.load(table.catalog(), table.tenantId(), table.table()).version()).isEqualTo(1);
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test
    void droppedAndRecreatedTableCannotAcceptOldIncarnation() throws Exception {
        var prepared = prepare("recreated", "recreated", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var old = prepared.allocation().output().table();
        var publication = fixture.registerObjects(prepared.allocation().executionId(), prepared.pages().get(0));
        store.drop(old);
        var replacement = new LanceTableHandle(old.catalog(), old.tenantId(), old.table(), UUID.randomUUID().toString(),
                old.datasetUri(), old.columns(), Map.of(), old.schemaFence(), 0, 1, 0);
        store.create(replacement);
        assertThatThrownBy(() -> fixture.acceptRegistered(prepared.allocation().executionId(), publication))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("changed generation");
        assertThat(store.load(replacement.catalog(), replacement.tenantId(), replacement.table()).tableId())
                .isEqualTo(replacement.tableId());
    }

    private LancePgSdkTestInputs.Prepared prepare(String dataset, String writeId, LanceWriteOperation operation,
            long version, int... values) throws Exception {
        return LancePgSdkTestInputs.write(fixture, warehouse, warehouse.resolve(dataset + ".lance").toString(),
                writeId, operation, version, values);
    }
    private void publish(String execution, PreparedSourcePublication publication, AtomicInteger winners, AtomicInteger conflicts) {
        try { fixture.acceptRegistered(execution, publication); winners.incrementAndGet(); }
        catch (IllegalStateException conflict) {
            if (!conflict.getMessage().contains("CAS conflict")) throw conflict;
            conflicts.incrementAndGet();
        }
    }
}
