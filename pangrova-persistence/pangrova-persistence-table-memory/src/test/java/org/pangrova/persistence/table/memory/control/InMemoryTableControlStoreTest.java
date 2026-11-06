package org.pangrova.persistence.table.memory.control;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.persistence.table.api.control.TableControlException;
import org.pangrova.table.api.ownership.OwnershipMode;
import org.pangrova.table.api.ownership.TableControlProvenance;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.ownership.TableControlState;
import org.pangrova.table.api.observation.TableVersionObservation;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryTableControlStoreTest {
    private static final Instant NOW = Instant.parse("2026-11-01T00:00:00Z");

    @Test
    void observationHistoryFollowsAcceptedRevisionsDespiteClockRollbackOrTies() {
        var store = new InMemoryTableControlStore();
        var record = record("cat", "db", "orders");
        var first = new TableVersionObservation(record.tableId(), "v1", null, "SYNC", NOW);
        var second = new TableVersionObservation(record.tableId(), "v2", "v1", "SYNC", NOW.minusSeconds(30));
        var third = new TableVersionObservation(record.tableId(), "v3", "v2", "SYNC", NOW.minusSeconds(30));
        store.saveWithObservation(record.toBuilder().version("v1", NOW).build(), first);
        store.saveWithObservation(record.toBuilder().metadataRevision(record.metadataRevision() + 1)
                .version("v2", second.observedAt()).build(), second);
        store.saveWithObservation(record.toBuilder().metadataRevision(record.metadataRevision() + 2)
                .version("v3", third.observedAt()).build(), third);

        assertThat(store.listVersionObservations(record.tableId(), 10)).containsExactly(third, second, first);
        assertThat(store.listVersionObservations(record.tableId(), 2)).containsExactly(third, second);
        assertThat(store.listVersionObservations(record.tableId(), 0)).isEmpty();
        assertThat(store.listVersionObservations(record.tableId(), -1)).isEmpty();
        assertThat(store.listVersionObservations(UUID.randomUUID(), 10)).isEmpty();
        assertThat(store.findById(record.tableId()).orElseThrow().versionToken()).isEqualTo("v3");
    }

    @Test
    void distinctNamesContainingSeparatorsNeverShareReservations() {
        var store = new InMemoryTableControlStore();
        var first = record("cat\u001Fdb", "schema", "orders");
        var second = record("cat", "db\u001Fschema", "orders");
        store.save(first);
        store.save(second);

        assertThat(store.find(first.catalog(), first.schema(), first.table())).contains(first);
        assertThat(store.find(second.catalog(), second.schema(), second.table())).contains(second);
        store.save(first.toBuilder().metadataRevision(2).state(TableControlState.DROPPED).build());
        assertThat(store.find(first.catalog(), first.schema(), first.table())).isEmpty();
        assertThat(store.find(second.catalog(), second.schema(), second.table())).contains(second);
    }

    @Test
    void rejectedStaleObservationHasTheSameConflictCodeAsJdbcAndChangesNothing() {
        var store = new InMemoryTableControlStore();
        var record = record("cat", "db", "orders");
        store.save(record);
        var observation = new TableVersionObservation(record.tableId(), "v1", null, "SYNC", NOW);

        assertThatThrownBy(() -> store.saveWithObservation(record, observation))
                .isInstanceOfSatisfying(TableControlException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.TABLE_CONTROL_STALE_WRITE));
        assertThat(store.findById(record.tableId())).contains(record);
        assertThat(store.listVersionObservations(record.tableId(), 10)).isEmpty();
    }

    @Test
    void historyIsIsolatedOrderedAndBoundedAndInvalidObservationCannotAdvanceRevision() {
        var store = new InMemoryTableControlStore();
        var record = record("cat", "db", "orders");
        var first = new TableVersionObservation(record.tableId(), "v1", null, "SYNC", NOW);
        store.saveWithObservation(record, first);
        var advanced = record.toBuilder().metadataRevision(2).version("v2", NOW.plusSeconds(1)).build();
        var second = new TableVersionObservation(record.tableId(), "v2", "v1", "SYNC", NOW.plusSeconds(1));
        store.saveWithObservation(advanced, second);

        assertThat(store.listVersionObservations(record.tableId(), 10)).containsExactly(second, first);
        assertThat(store.listVersionObservations(record.tableId(), 1)).containsExactly(second);
        assertThat(store.listVersionObservations(record.tableId(), 0)).isEmpty();
        assertThat(store.listVersionObservations(UUID.randomUUID(), 10)).isEmpty();
        assertThatThrownBy(() -> store.saveWithObservation(advanced.toBuilder().metadataRevision(3).build(), first))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.findById(record.tableId())).contains(advanced);
        assertThat(new InMemoryTableControlStore().listAll()).isEmpty();
    }

    private static TableControlRecord record(String catalog, String schema, String table) {
        return TableControlRecord.builder().tableId(UUID.randomUUID()).name(catalog, schema, table)
                .format("ICEBERG").ownershipMode(OwnershipMode.MANAGED).state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED).metadataRevision(1).version("v1", NOW)
                .createdAt(NOW).updatedAt(NOW).build();
    }
}
