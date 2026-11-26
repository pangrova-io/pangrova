package org.pangrova.persistence.indexing.postgresql.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.api.metadata.IndexDescriptors.MaintenancePage;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcIndexDescriptorMaintenanceTest extends ContainerTestBase {
    private static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");
    private DataSource dataSource;
    private JdbcIndexMetadataStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource(
                "descriptor_maintenance_" + UUID.randomUUID().toString().replace("-", ""));
        store = reopen();
    }

    @Test
    void keysetProgressSurvivesRestartAndStateChangesWithoutOffsetSkips() {
        for (String id : List.of("b", "d", "f", "h", "j")) register(id, IndexState.READY, NOW);
        MaintenancePage first = store.loadMaintenancePage(IndexState.READY, null, 2);
        assertIds(first, "b", "d");
        assertThat(first.exhausted()).isFalse();
        assertThat(store.completeMaintenancePage(first)).isTrue();
        store.updateIndexState("b", IndexState.STALE.name(), NOW.plusSeconds(1));
        store.updateIndexState("d", IndexState.STALE.name(), NOW.plusSeconds(1));
        register("a", IndexState.READY, NOW); // now behind the cursor
        register("g", IndexState.READY, NOW); // inside the initial upper bound
        register("z", IndexState.READY, NOW); // beyond the initial upper bound
        store = reopen();

        var second = store.loadMaintenancePage(IndexState.READY, null, 2);
        assertIds(second, "f", "g");
        assertThat(store.completeMaintenancePage(first)).isFalse();
        assertThat(store.completeMaintenancePage(second)).isTrue();
        var last = store.loadMaintenancePage(IndexState.READY, null, 2);
        assertIds(last, "h", "j");
        assertThat(store.completeMaintenancePage(last)).isTrue();
        var end = store.loadMaintenancePage(IndexState.READY, null, 2);
        assertIds(end);
        assertThat(end.exhausted()).isTrue();
        assertThat(store.completeMaintenancePage(end)).isTrue();
        assertThat(store.completeMaintenancePage(first)).isFalse(); // wrap cannot ABA
        assertIds(store.loadMaintenancePage(IndexState.READY, null, 20), "a", "f", "g", "h", "j", "z");
        assertIds(store.loadMaintenancePage(IndexState.STALE, null, 10), "b", "d");
    }

    @Test
    void droppedCutoffIsStrictAndOnlyCompletedPagesAdvance() {
        register("a-old", IndexState.DROPPED, NOW.minusSeconds(2));
        register("b-equal", IndexState.DROPPED, NOW);
        register("c-old", IndexState.DROPPED, NOW.minusSeconds(1));
        register("d-recent", IndexState.DROPPED, NOW.plusSeconds(1));
        register("e-ready", IndexState.READY, NOW.minusSeconds(10));
        var first = store.loadMaintenancePage(IndexState.DROPPED, NOW, 1);
        assertIds(first, "a-old");
        store = reopen(); // crash before processing completion replays the page
        assertIds(store.loadMaintenancePage(IndexState.DROPPED, NOW, 1), "a-old");
        assertThat(store.completeMaintenancePage(first)).isTrue();
        var second = store.loadMaintenancePage(IndexState.DROPPED, NOW, 5);
        assertIds(second, "c-old");
        assertThat(second.exhausted()).isTrue();
        assertThat(store.completeMaintenancePage(second)).isTrue();
        var fractionalCutoff = store.loadMaintenancePage(IndexState.DROPPED, NOW.plusNanos(1), 10);
        assertIds(fractionalCutoff, "a-old", "b-equal", "c-old");
        assertThat(store.completeMaintenancePage(fractionalCutoff)).isTrue();
        assertIds(store.loadMaintenancePage(IndexState.DROPPED, NOW.plusSeconds(2), 10),
                "a-old", "b-equal", "c-old", "d-recent");
    }

    private JdbcIndexMetadataStore reopen() {
        return new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    private void register(String id, IndexState state, Instant timestamp) {
        store.registerIndex(new IndexDescriptor(id, id, "lake", "raw", "items", List.of("value"),
                IndexType.BLOOM_FILTER, Map.of(), state, null, 0, null, null,
                timestamp, timestamp, "tenant"));
    }

    private static void assertIds(MaintenancePage page, String... ids) {
        assertThat(page.candidates()).extracting(IndexDescriptor::indexId).containsExactly(ids);
    }
}
