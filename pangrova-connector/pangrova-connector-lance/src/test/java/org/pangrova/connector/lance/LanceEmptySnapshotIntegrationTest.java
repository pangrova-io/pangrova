package org.pangrova.connector.lance;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testing.ContainerTestBase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK manifest publication and PostgreSQL directory, without a simulated Native control port. */
class LanceEmptySnapshotIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void selectedEmptySnapshotSurvivesFirstPublicationButNotTableReplacement() throws Exception {
        var postgres = LanceCatalogTestDatabase.instance();
        var store = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        store.createSchema("lance_catalog", "tenant-a", "db");
        var schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
        var initial = table(schema, "empty_" + UUID.randomUUID());
        store.create(initial);
        var metadata = new LanceMetadata("lance_catalog", warehouse.toString(), () -> store, Optional.empty());
        var splits = new LanceSplitManager(Optional.empty(), () -> store, "lance_catalog", warehouse.toString());
        var session = LanceSplitManagerTest.session("tenant-a");
        var selected = metadata.bindRead(session, metadata.getTableHandle(session, initial.table()));
        assertThat(selected.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);

        publishSdkManifest(store, initial, schema);

        assertThat(store.load(initial.catalog(), initial.tenantId(), initial.table()).version()).isEqualTo(1);
        metadata.validateReadBinding(session, selected);
        assertThat(metadata.getTableStatistics(session, selected.handle()).rowCount()).isZero();
        assertThat(metadata.getTableMetadata(session, selected.handle()).columns()).containsExactly("id");
        var source = splits.getSplits(ConnectorTransactionHandle.EMPTY, session, selected.handle(),
                DynamicFilter.ALL, Constraint.EMPTY);
        try {
            assertThat(source.isFinished()).isTrue();
            assertThat(source.getNextBatch(1)).isEmpty();
        } finally {
            source.close();
        }
        assertThatThrownBy(() -> splits.getSplits(ConnectorTransactionHandle.EMPTY,
                LanceSplitManagerTest.session("tenant-b"), selected.handle(), DynamicFilter.ALL, Constraint.EMPTY))
                .isInstanceOf(SecurityException.class);
        var current = store.load(initial.catalog(), initial.tenantId(), initial.table());
        store.drop(current);
        assertThatThrownBy(() -> splits.getSplits(ConnectorTransactionHandle.EMPTY, session, selected.handle(),
                DynamicFilter.ALL, Constraint.EMPTY)).hasMessageContaining("incarnation");
        store.create(table(schema, initial.table().table()));
        assertThatThrownBy(() -> metadata.getTableMetadata(session, selected.handle()))
                .hasMessageContaining("incarnation");
    }

    private LanceTableHandle table(Schema schema, String name) throws Exception {
        String identity = UUID.randomUUID().toString();
        return new LanceTableHandle("lance_catalog", "tenant-a", new SchemaTableName("db", name), identity,
                warehouse.resolve(identity + ".lance").toString(),
                List.of(LanceColumn.from(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT))), Map.of(),
                MessageDigest.getInstance("SHA-256").digest(schema.serializeAsMessage()), 0, 1, 0);
    }

    private static void publishSdkManifest(LanceCatalogStore store, LanceTableHandle initial, Schema schema)
            throws Exception {
        long version;
        long rows;
        try (var allocator = new RootAllocator(1024 * 1024);
                var dataset = Dataset.create(allocator, initial.datasetUri(), schema,
                        new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build())) {
            version = dataset.version();
            rows = dataset.countRows();
            assertThat(version).isEqualTo(1);
            assertThat(rows).isZero();
        }
        byte[] manifest;
        try (var paths = Files.list(Path.of(initial.datasetUri()).resolve("_versions"))) {
            var manifests = paths.filter(path -> path.getFileName().toString().endsWith(".manifest")).toList();
            assertThat(manifests).hasSize(1);
            manifest = Files.readAllBytes(manifests.get(0));
        }
        assertThat(manifest).isNotEmpty();
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(manifest);
        var allocation = new NativeWriteAllocation(initial.datasetUri(), NativeWriteAllocation.Operation.CREATE,
                0, initial.schemaFence(), UUID.randomUUID().toString(), "staging/empty-snapshot",
                UUID.randomUUID().toString(), List.of("id"));
        store.publish(new LanceOutputHandle(initial, allocation, "INSERT"), HexFormat.of().formatHex(digest),
                version, digest, rows);
        assertThat(store.loadSnapshot((LanceTableHandle) initial.withTimeTravelSnapshot(version)).manifestDigest())
                .containsExactly(digest);
    }
}
