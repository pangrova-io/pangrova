package org.pangrova.coordinator.query.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.task.write.WriteCommitReconciler;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.coordination.JdbcCopyFileClaimStore;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.security.authz.Privilege;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.CopyFileClaim;
import org.pangrova.spi.coordination.CopyFileClaimStore;
import org.pangrova.spi.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;
import org.pangrova.spi.security.UserIdentity;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Real PostgreSQL plus Iceberg catalog recovery of an uncertain COPY publication. */
class CopyFileCommitRecoveryIntegrationTest {

    private static final String EXECUTION = "copy-recovery-execution";

    @TempDir
    Path warehouse;

    private CatalogRegistry registry;

    @AfterEach
    void shutdownRegistry() {
        if (registry != null) {
            registry.shutdown();
        }
    }

    @Test
    void markerCommitBeforePostgresProofReconcilesButRollbackNeverLooksLoaded() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_recovery_iceberg");
        var journal = new JdbcWriteCommitJournal(source);
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        try (var catalog = new HadoopCatalog(configuration, warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
            Table table = catalog.createTable(TableIdentifier.of("db", "items"), schema);
            table.newAppend().appendFile(dataFile(table, "base.parquet", 1L)).commit();
            table.refresh();
            long baseSnapshot = table.currentSnapshot().snapshotId();

            var connector = new IcebergConnector("lake", catalog);
            var descriptor = new CatalogDescriptor("lake", CatalogType.CONNECTOR, "iceberg",
                    CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()), "",
                    Set.of("tenant-a"), "catalog-incarnation", 1L);
            registry = new CatalogRegistry(new JdbcMetadataStore(source),
                    registered -> new ConnectorCatalogHandle(registered, connector), null, null);
            registry.register(descriptor);
            var catalogHandle = (ConnectorCatalogHandle) registry.getCatalog("lake");
            var actor = new CoordinatorSessionContext(EXECUTION, Map.of(),
                    new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant-a");
            var session = SessionContextConverter.toConnectorSession(actor, "lake", "db");
            var metadata = connector.getMetadata(session);
            var tableHandle = metadata.getTableHandle(session, new SchemaTableName("db", "items"));
            var output = (IcebergOutputTableHandle) metadata.beginInsert(session, tableHandle);

            CopyFileClaim claim = new CopyFileClaim("tenant-a", output.tableUuid(),
                    "s3://bucket/data/file.parquet", "version-1", "a".repeat(64), 4L,
                    "profile", "00000000-0000-0000-0000-000000000001", 1L, 0L, EXECUTION);
            assertThat(new JdbcCopyFileClaimStore(source).claim(claim))
                    .isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);
            DataFile added = dataFile(table, "copy.parquet", 2L);
            byte[] fragment = ("[" + ContentFileParser.toJson(added, table.spec()) + "]")
                    .getBytes(StandardCharsets.UTF_8);
            List<byte[]> fragments = List.of(fragment);
            byte[] binding = ConnectorWriteBindingFactory.create(catalogHandle, output).toByteArray();
            WriteAuthorizationReceipt receipt = receipt(metadata.getTableMetadata(session, tableHandle));
            String fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                    EXECUTION, "tenant-a", "lake", "db.items", "INSERT", baseSnapshot,
                    binding, "alice", receipt, claim.commitBinding()), fragments);
            WriteCommitEntry intent = new WriteCommitEntry(EXECUTION, "lake", "db.items",
                    WriteCommitEntry.State.PENDING, 1_000L, fragments, "INSERT", fingerprint,
                    "tenant-a", "alice", receipt, baseSnapshot, binding, fingerprint,
                    null, null, 0L, null, claim.commitBinding());
            journal.recordIntent(intent);

            // The catalog commit succeeds, then the coordinator dies before writing
            // its PostgreSQL publication proof or terminal journal state.
            metadata.finishDistributedWrite(session, output, fragments, intent.commitContext());
            table.refresh();
            long publishedSnapshot = table.currentSnapshot().snapshotId();
            assertThat(journal.findCopyFilePublication(intent)).isEmpty();
            assertThat(journal.getEntry(EXECUTION).orElseThrow().state())
                    .isEqualTo(WriteCommitEntry.State.PENDING);

            var reconciler = new DefaultWriteCommitReconciler(journal,
                    name -> (ConnectorCatalogHandle) registry.getCatalog(name));
            assertThat(reconciler.reconcile(intent))
                    .isEqualTo(WriteCommitReconciler.RecoveryOutcome.COMMITTED);
            assertThat(journal.findCopyFilePublication(intent).orElseThrow().snapshotId())
                    .isEqualTo(publishedSnapshot);
            table.refresh();
            assertThat(table.currentSnapshot().snapshotId()).isEqualTo(publishedSnapshot);
            journal.markCommitted(EXECUTION);

            table.manageSnapshots().rollbackTo(baseSnapshot).commit();
            assertThat(reconciler.reconcile(journal.getEntry(EXECUTION).orElseThrow()))
                    .isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
            table.refresh();
            assertThat(table.currentSnapshot().snapshotId()).isEqualTo(baseSnapshot);
        }
    }

    private static DataFile dataFile(Table table, String name, long id) throws Exception {
        var output = table.io().newOutputFile(table.location() + "/data/" + name);
        var writer = Parquet.writeData(output).schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            var row = GenericRecord.create(table.schema());
            row.setField("id", id);
            writer.write(row);
        }
        return writer.toDataFile();
    }

    private static WriteAuthorizationReceipt receipt(ConnectorTableMetadata metadata) {
        var target = new QualifiedTableName("lake", "db", "items");
        var declaration = WriteDeclarationSnapshot.capture(target, metadata, new byte[0], false);
        var permission = new DeferredAuthorizationScope.TablePermission(target, Set.of(), Privilege.INSERT,
                DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty());
        return DeferredAuthorizationScopeCodec.encodeWriteReceipt(new DeferredAuthorizationScope(
                List.of(permission), Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(
                        declaration, List.of()))));
    }
}
