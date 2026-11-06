package org.pangrova.connector.paimon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.materialization.ChangeProof;
import org.pangrova.connector.api.materialization.ChangeSetMode;
import org.pangrova.connector.api.materialization.ChangeSetPlan;
import org.pangrova.connector.api.materialization.MaterializationCapabilities;
import org.pangrova.connector.api.materialization.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

class PaimonMaterializationSourceProviderTest {

    private static final String POLICY = "policy:test";
    private static final SourceRef SOURCE = new SourceRef("paimon", "lake", "db", "items");

    @TempDir
    Path warehouse;

    @Test
    void pinsAreOwnerScopedAndSurviveCatalogRestart() throws Exception {
        try (Catalog catalog = catalog()) {
            createTable(catalog, false);
            commit(catalog, 1);
            var provider = new PaimonMaterializationSourceProvider("lake", catalog);
            SourceSnapshotRef observed = describe(provider, catalog);
            PinnedSource first = provider.pin(SOURCE, "t1", observed, "attempt-1");
            PinnedSource second = provider.pin(SOURCE, "t1", observed, "attempt-2");
            assertThat(first.snapshot().snapshotToken())
                    .isEqualTo(second.snapshot().snapshotToken());

            provider.unpin(first);
            provider.unpin(first);
            assertThat(holds(catalog)).hasSize(1);
        }
        try (Catalog reopened = catalog()) {
            assertThat(holds(reopened)).hasSize(1);
            var provider = new PaimonMaterializationSourceProvider("lake", reopened);
            SourceSnapshotRef observed = provider.describe(SOURCE, "t1", "1", POLICY);
            PinnedSource pin = provider.pin(SOURCE, "t1", observed, "attempt-2");
            provider.unpin(pin);
            assertThat(holds(reopened)).isEmpty();
        }
    }

    @Test
    void emptyTableCannotBePinned() throws Exception {
        try (Catalog catalog = catalog()) {
            createTable(catalog, false);
            var provider = new PaimonMaterializationSourceProvider("lake", catalog);
            assertThatThrownBy(() -> provider.describe(SOURCE, "t1", "1", POLICY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
            assertThatThrownBy(() -> provider.pin(SOURCE, "t1",
                    new SourceSnapshotRef("paimon", "1", "schema", POLICY,
                            java.time.Instant.now(), true), "attempt-1"))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
        }
    }

    @Test
    void provesAppendOnlyIntervalAndRejectsUnsupportedModes() throws Exception {
        try (Catalog catalog = catalog()) {
            createTable(catalog, false);
            commit(catalog, 1);
            var provider = new PaimonMaterializationSourceProvider("lake", catalog);
            PinnedSource from = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-1");
            commit(catalog, 2);
            PinnedSource to = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-2");

            ChangeSetPlan plan = provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY);
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
    void overwriteIntervalIsNotProvable() throws Exception {
        try (Catalog catalog = catalog()) {
            createTable(catalog, false);
            commit(catalog, 1);
            var provider = new PaimonMaterializationSourceProvider("lake", catalog);
            PinnedSource from = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-1");
            overwrite(catalog, 2);
            PinnedSource to = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-2");

            assertThatThrownBy(() -> provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.PROOF_INCOMPLETE);

            ChangeSetPlan unproven = new ChangeSetPlan(SOURCE, from.snapshot(), to.snapshot(),
                    ChangeSetMode.APPEND_ONLY, "plan:overwrite", java.time.Instant.now(),
                    java.util.List.of());
            assertThat(provider.verify(unproven).complete()).isFalse();
        }
    }

    @Test
    void primaryKeyTableNeverClaimsAppendOnly() throws Exception {
        try (Catalog catalog = catalog()) {
            createTable(catalog, true);
            commit(catalog, 1);
            var provider = new PaimonMaterializationSourceProvider("lake", catalog);
            MaterializationCapabilities capabilities = provider.capabilities(SOURCE);
            assertThat(capabilities.snapshotPin()).isTrue();
            assertThat(capabilities.appendOnlyProof()).isFalse();
            assertThat(capabilities.changeSet()).isTrue();
            assertThat(capabilities.supports(ChangeSetMode.APPEND_ONLY)).isFalse();

            PinnedSource from = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-1");
            commit(catalog, 2);
            PinnedSource to = provider.pin(SOURCE, "t1", describe(provider, catalog), "attempt-2");
            assertThatThrownBy(() -> provider.changeSet(from, to, ChangeSetMode.APPEND_ONLY))
                    .isInstanceOf(MaterializationSourceException.class)
                    .extracting(failure -> ((MaterializationSourceException) failure).reason())
                    .isEqualTo(MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED);
        }
    }

    private static SourceSnapshotRef describe(
            PaimonMaterializationSourceProvider provider, Catalog catalog) {
        try {
            FileStoreTable table = (FileStoreTable) catalog.getTable(
                    Identifier.create("db", "items"));
            return provider.describe(SOURCE, "t1",
                    Long.toString(table.snapshotManager().latestSnapshotId()), POLICY);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private Catalog catalog() {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private static void createTable(Catalog catalog, boolean primaryKey) throws Exception {
        catalog.createDatabase("db", false);
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("value", DataTypes.STRING())
                .option("bucket", "1");
        if (primaryKey) {
            builder.primaryKey("id");
        } else {
            builder.option("bucket-key", "id");
        }
        catalog.createTable(Identifier.create("db", "items"), builder.build(), false);
    }

    private static void commit(Catalog catalog, long id) throws Exception {
        Table table = catalog.getTable(Identifier.create("db", "items"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
             BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString("val-" + id)));
            commit.commit(write.prepareCommit());
        }
    }

    private static void overwrite(Catalog catalog, long id) throws Exception {
        Table table = catalog.getTable(Identifier.create("db", "items"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder().withOverwrite(Map.of());
        try (BatchTableWrite write = builder.newWrite();
             BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString("val-" + id)));
            commit.commit(write.prepareCommit());
        }
    }

    private static List<String> holds(Catalog catalog) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", "items"));
        return table.tagManager().tags().values().stream()
                .flatMap(List::stream)
                .filter(name -> name.startsWith("pangrova_materialization_"))
                .toList();
    }
}
