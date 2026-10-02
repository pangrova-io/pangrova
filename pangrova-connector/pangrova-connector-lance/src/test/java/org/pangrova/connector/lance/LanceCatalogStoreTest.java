package org.pangrova.connector.lance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL catalog CAS, retained identity and concurrent publication tests. */
class LanceCatalogStoreTest extends ContainerTestBase {
    private LanceCatalogStore store;

    @BeforeEach void setUp() {
        var postgres = LanceCatalogTestDatabase.instance();
        store = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        store.createSchema("lance_catalog", "tenant-a", "db");
    }

    @Test
    void persistsEmptySchemaWithoutLeakingItToAnotherTenant() {
        String schema = "empty_" + java.util.UUID.randomUUID().toString();
        store.createSchema("lance_catalog", "tenant-a", schema);
        assertThat(store.listSchemas("lance_catalog", "tenant-a")).contains(schema);
        assertThat(store.listSchemas("lance_catalog", "tenant-b")).doesNotContain(schema);
        var postgres = LanceCatalogTestDatabase.instance();
        var restarted = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        assertThat(restarted.listSchemas("lance_catalog", "tenant-a")).contains(schema);
        assertThatThrownBy(() -> store.createSchema("lance_catalog", "tenant-a", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void persistentDirectoryIsTenantIsolatedAndPinnedHandleKeepsOldVersion() {
        var table = LanceOutputHandleCodecTest.table("tenant-a", "isolated");
        store.create(table);
        assertThat(store.load(table.catalog(), "tenant-b", table.table())).isNull();
        store.publish(LanceOutputHandleCodecTest.output(table), "a".repeat(64), 1, new byte[32], 2);
        var postgres = LanceCatalogTestDatabase.instance();
        var restarted = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        var visible = restarted.load(table.catalog(), table.tenantId(), table.table());
        assertThat(visible.version()).isEqualTo(1);
        assertThat(visible.rowCount()).isEqualTo(2);
        assertThat(table.version()).isZero();
    }

    @Test
    void sameCommitReplayIsIdempotentAndChangedManifestIsRejected() {
        var table = LanceOutputHandleCodecTest.table("tenant-a", "replay");
        var output = LanceOutputHandleCodecTest.output(table);
        store.create(table);
        store.publish(output, "b".repeat(64), 1, new byte[32], 3);
        store.publish(output, "b".repeat(64), 1, new byte[32], 3);
        assertThat(store.load(table.catalog(), table.tenantId(), table.table()).revision()).isEqualTo(2);
        byte[] changed = new byte[32];
        changed[0] = 1;
        assertThatThrownBy(() -> store.publish(output, "b".repeat(64), 1, changed, 3))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("replay conflict");
    }

    @Test
    void historicalPublicationRetainsExactSchemaRowsAndNativeDigestAcrossRestart() {
        var initial = LanceOutputHandleCodecTest.table("tenant-a", "history_" + java.util.UUID.randomUUID());
        store.create(initial);
        byte[] firstDigest = new byte[32];
        firstDigest[0] = 7;
        store.publish(LanceOutputHandleCodecTest.output(initial), "1".repeat(64), 1, firstDigest, 2);
        var first = store.load(initial.catalog(), initial.tenantId(), initial.table());
        byte[] secondDigest = new byte[32];
        secondDigest[0] = 8;
        store.publish(LanceOutputHandleCodecTest.output(first), "2".repeat(64), 2, secondDigest, 3);
        var current = store.load(initial.catalog(), initial.tenantId(), initial.table());
        var selector = (LanceTableHandle) current.withTimeTravelSnapshot(1);
        assertThat(selector).isNotSameAs(current);
        var postgres = LanceCatalogTestDatabase.instance();
        var restarted = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        var historical = restarted.loadSnapshot(selector);
        assertThat(historical.table().version()).isEqualTo(1);
        assertThat(historical.table().rowCount()).isEqualTo(2);
        assertThat(historical.table().columns()).containsExactlyElementsOf(initial.columns());
        assertThat(historical.manifestDigest()).containsExactly(firstDigest);
        assertThat(current.rowCount()).isEqualTo(5);
        firstDigest[0] = 99;
        assertThat(historical.manifestDigest()[0]).isEqualTo((byte) 7);
        assertThatThrownBy(() -> restarted.loadSnapshot((LanceTableHandle) current.withTimeTravelSnapshot(3)))
                .hasMessageContaining("exact snapshot publication is absent");
        assertThatThrownBy(() -> current.withTimeTravelSnapshot(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> current.withTimeTravelSnapshot(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LanceSnapshotResolver(() -> restarted, java.util.Optional.empty(),
                "lance_catalog", "s3://warehouse")
                .resolve(LanceSplitManagerTest.session("tenant-a"), selector))
                .hasMessageContaining("requires Native control");
        assertThatThrownBy(() -> new LanceSnapshotResolver(() -> restarted, java.util.Optional.empty(),
                "lance_catalog", "s3://warehouse")
                .resolve(LanceSplitManagerTest.session("tenant-b"), selector)).isInstanceOf(SecurityException.class);
    }

    @Test
    void concurrentPublicationsFromOneBaseHaveExactlyOneWinner() throws Exception {
        var table = LanceOutputHandleCodecTest.table("tenant-a", "concurrent");
        store.create(table);
        var executor = ManagedExecutorFactory.newFixedThreadPool("lance-catalog-cas-test", 2);
        AtomicInteger winners = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        try {
            var first = executor.submit(() -> publish(table, "c".repeat(64), winners, conflicts));
            var second = executor.submit(() -> publish(table, "d".repeat(64), winners, conflicts));
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
            assertThat(winners).hasValue(1);
            assertThat(conflicts).hasValue(1);
            assertThat(store.load(table.catalog(), table.tenantId(), table.table()).version()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void droppedAndRecreatedTableCannotAcceptOldIncarnation() {
        var old = LanceOutputHandleCodecTest.table("tenant-a", "recreated");
        store.create(old);
        store.drop(old);
        var replacement = LanceOutputHandleCodecTest.table("tenant-a", "recreated");
        store.create(replacement);
        assertThatThrownBy(() -> store.publish(LanceOutputHandleCodecTest.output(old), "e".repeat(64), 1, new byte[32], 1))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CAS conflict");
        assertThat(store.load(replacement.catalog(), replacement.tenantId(), replacement.table()).tableId())
                .isEqualTo(replacement.tableId());
    }

    private void publish(LanceTableHandle table, String fingerprint, AtomicInteger winners, AtomicInteger conflicts) {
        try {
            store.publish(LanceOutputHandleCodecTest.output(table), fingerprint, 1, new byte[32], 1);
            winners.incrementAndGet();
        } catch (IllegalStateException conflict) {
            if (!conflict.getMessage().contains("CAS conflict")) throw conflict;
            conflicts.incrementAndGet();
        }
    }
}
