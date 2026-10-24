package org.pangrova.connector.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.MaterializationCapabilities;
import org.pangrova.spi.materialization.MaterializationSourceException;
import org.pangrova.spi.materialization.PinnedSource;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

class IcebergMaterializationSourceProviderTest {

    private static final String POLICY = "policy:test";

    @TempDir
    Path warehouse;

    @Test
    void pinsAreOwnerScopedAndSurviveCatalogRestart() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        SourceRef source = new SourceRef("iceberg", "lake", "db", "items");
        PinnedSource first;
        PinnedSource second;
        try (var catalog = catalog()) {
            Table table = table(catalog);
            append(table, "first");
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);

            SourceSnapshotRef observed = describe(provider, source, table);
            first = provider.pin(source, "t1", observed, "attempt-1");
            second = provider.pin(source, "t1", observed, "attempt-2");
            assertThat(first.snapshot().snapshotToken())
                    .isEqualTo(second.snapshot().snapshotToken())
                    .isEqualTo(Long.toString(table.currentSnapshot().snapshotId()));

            provider.unpin(first);
            assertThat(pins(catalog, TableIdentifier.of("db", "items"))).hasSize(1);
            provider.unpin(first);
            assertThat(pins(catalog, TableIdentifier.of("db", "items"))).hasSize(1);
        }
        try (var reopened = catalog()) {
            assertThat(pins(reopened, TableIdentifier.of("db", "items"))).hasSize(1);
            var provider = new IcebergMaterializationSourceProvider("lake", reopened);
            provider.unpin(second);
            assertThat(pins(reopened, TableIdentifier.of("db", "items"))).isEmpty();
        }
    }

    @Test
    void emptyTablePinsItsIdentityAndRejectsLaterWrites() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        try (var catalog = catalog()) {
            Table table = table(catalog);
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);
            SourceRef source = new SourceRef("iceberg", "lake", "db", "items");
            assertThatThrownBy(() -> provider.describe(source, "t1", "1", POLICY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);

            SourceSnapshotRef empty = provider.describeEmpty(
                    source, "t1", emptyIdentity(table), POLICY);
            assertThat(empty.isEmptySource()).isTrue();
            assertThat(empty.pinnable()).isTrue();
            PinnedSource pinned = provider.pin(source, "t1", empty, "attempt-1");
            provider.unpin(pinned);

            table.refresh();
            append(table, "first");
            assertThatThrownBy(() -> provider.pin(source, "t1", empty, "attempt-2"))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.SHAPE_MISMATCH);
        }
    }

    @Test
    void provesAppendOnlyIntervalAndRejectsUnsupportedModes() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        SourceRef source = new SourceRef("iceberg", "lake", "db", "items");
        try (var catalog = catalog()) {
            Table table = table(catalog);
            append(table, "first");
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);
            PinnedSource from = provider.pin(source, "t1", describe(provider, source, table), "attempt-1");
            table.refresh();
            append(table, "second");
            PinnedSource to = provider.pin(source, "t1", describe(provider, source, table), "attempt-2");

            ChangeSetPlan plan = provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY);
            assertThat(plan.mode()).isEqualTo(ChangeSetMode.APPEND_ONLY);
            ChangeProof proof = provider.verify(plan);
            assertThat(proof.complete()).isTrue();
            proof.requireComplete();

            assertThatThrownBy(() ->
                    provider.changeSet(from, to, ChangeSetMode.PARTITION_REPLACE))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED);
        }
    }

    @Test
    void deleteIntervalIsNotProvable() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        SourceRef source = new SourceRef("iceberg", "lake", "db", "items");
        try (var catalog = catalog()) {
            Table table = table(catalog);
            DataFile file = append(table, "first");
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);
            PinnedSource from = provider.pin(source, "t1", describe(provider, source, table), "attempt-1");
            table.refresh();
            table.newDelete().deleteFile(file).commit();
            PinnedSource to = provider.pin(source, "t1", describe(provider, source, table), "attempt-2");

            assertThatThrownBy(() -> provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.PROOF_INCOMPLETE);

            ChangeSetPlan unproven = new ChangeSetPlan(source, from.snapshot(), to.snapshot(),
                    ChangeSetMode.APPEND_ONLY, "plan:delete", java.time.Instant.now());
            assertThat(provider.verify(unproven).complete()).isFalse();
        }
    }

    @Test
    void schemaEvolutionInsideIntervalBreaksShape() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        SourceRef source = new SourceRef("iceberg", "lake", "db", "items");
        try (var catalog = catalog()) {
            Table table = table(catalog);
            append(table, "first");
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);
            PinnedSource from = provider.pin(source, "t1", describe(provider, source, table), "attempt-1");
            table.refresh();
            table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
            append(table, "second");
            PinnedSource to = provider.pin(source, "t1", describe(provider, source, table), "attempt-2");

            assertThatThrownBy(() -> provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.SHAPE_MISMATCH);
        }
    }

    @Test
    void declaresOnlyProvableCapabilities() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        try (var catalog = catalog()) {
            table(catalog);
            var provider = new IcebergMaterializationSourceProvider("lake", catalog);
            MaterializationCapabilities capabilities = provider.capabilities(
                    new SourceRef("iceberg", "lake", "db", "items"));
            assertThat(capabilities.snapshotPin()).isTrue();
            assertThat(capabilities.appendOnlyProof()).isTrue();
            assertThat(capabilities.changeSet()).isTrue();
            assertThat(capabilities.partitionReplace()).isFalse();
            assertThat(capabilities.affectedKey()).isFalse();
            assertThat(capabilities.cdc()).isFalse();
            assertThat(capabilities.supports(ChangeSetMode.APPEND_ONLY)).isTrue();
            assertThat(capabilities.supports(ChangeSetMode.PARTITION_REPLACE)).isFalse();
            assertThat(capabilities.supports(ChangeSetMode.CDC)).isFalse();
        }
    }

    private static String emptyIdentity(Table table) {
        var operations = (org.apache.iceberg.HasTableOperations) table;
        return org.apache.commons.codec.digest.DigestUtils.sha256Hex(
                operations.operations().current().uuid() + "\0"
                        + table.location() + "\0EMPTY");
    }

    private static SourceSnapshotRef describe(
            IcebergMaterializationSourceProvider provider, SourceRef source, Table table) {
        table.refresh();
        return provider.describe(source, "t1",
                Long.toString(table.currentSnapshot().snapshotId()), POLICY);
    }

    private HadoopCatalog catalog() {
        return new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    }

    private static Table table(HadoopCatalog catalog) {
        catalog.createNamespace(Namespace.of("db"));
        return catalog.createTable(TableIdentifier.of("db", "items"), schema());
    }

    private static DataFile append(Table table, String name) {
        DataFile file = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
        table.newFastAppend().appendFile(file).commit();
        return file;
    }

    private static java.util.List<org.apache.iceberg.SnapshotRef> pins(
            HadoopCatalog catalog, TableIdentifier id) {
        Table table = catalog.loadTable(id);
        table.refresh();
        return table.refs().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("pangrova_materialization_"))
                .map(java.util.Map.Entry::getValue)
                .toList();
    }
}
