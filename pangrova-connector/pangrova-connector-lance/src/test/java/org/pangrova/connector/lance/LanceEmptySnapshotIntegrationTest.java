package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real fixed SDK preparation and PostgreSQL acceptance, without a simulated Native control port. */
class LanceEmptySnapshotIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void selectedEmptySnapshotSurvivesFirstPublicationButNotTableReplacement() throws Exception {
        var fixture = new LanceManagedSourceFixture("empty_" + UUID.randomUUID(), warehouse);
        var sdk = new LanceNativeSdkFixture(fixture, warehouse);
        var input = LancePgSdkTestInputs.stream(warehouse, "empty-selected", 7);
        var allocation = sdk.allocate(input, warehouse.resolve("empty.lance").toString(), "empty-selected",
                LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, LancePgSdkTestInputs.SCHEMA);
        var store = fixture.store();
        var initial = allocation.output().table();
        var metadata = new LanceMetadata(initial.catalog(), warehouse.toString(), () -> store, Optional.empty());
        var splits = new LanceSplitManager(Optional.empty(), () -> store, initial.catalog(), warehouse.toString());
        var session = LanceSplitManagerTest.session("tenant-a");
        var selected = metadata.bindRead(session, metadata.getTableHandle(session, initial.table()));
        assertThat(selected.state()).isEqualTo(ReadSnapshotState.EMPTY);

        var fragment = sdk.write(input, allocation);
        var accepted = sdk.accept(allocation, sdk.prepare(allocation, java.util.List.of(fragment)));
        assertThat(accepted.getVersion()).isEqualTo(1);
        assertThat(store.load(initial.catalog(), initial.tenantId(), initial.table()).version()).isEqualTo(1);
        metadata.validateReadBinding(session, selected);
        assertThat(metadata.getTableStatistics(session, selected.handle()).rowCount()).isZero();
        assertThat(metadata.getTableMetadata(session, selected.handle()).columns()).containsExactly("id");
        var source = splits.getSplits(ConnectorTransactionHandle.empty(), session, selected.handle(),
                DynamicFilter.ALL, Constraint.EMPTY);
        try {
            assertThat(source.isFinished()).isTrue();
            assertThat(source.getNextBatch(1)).isEmpty();
        } finally { source.close(); }
        assertThatThrownBy(() -> splits.getSplits(ConnectorTransactionHandle.empty(),
                LanceSplitManagerTest.session("tenant-b"), selected.handle(), DynamicFilter.ALL, Constraint.EMPTY))
                .isInstanceOf(SecurityException.class);
        var current = store.load(initial.catalog(), initial.tenantId(), initial.table());
        store.drop(current);
        assertThatThrownBy(() -> splits.getSplits(ConnectorTransactionHandle.empty(), session, selected.handle(),
                DynamicFilter.ALL, Constraint.EMPTY)).hasMessageContaining("incarnation");
        store.create(new LanceTableHandle(initial.catalog(), initial.tenantId(), initial.table(), UUID.randomUUID().toString(),
                initial.datasetUri(), initial.columns(), Map.of(), initial.schemaFence(), 0, 1, 0));
        assertThatThrownBy(() -> metadata.getTableMetadata(session, selected.handle()))
                .hasMessageContaining("incarnation");
    }
}
