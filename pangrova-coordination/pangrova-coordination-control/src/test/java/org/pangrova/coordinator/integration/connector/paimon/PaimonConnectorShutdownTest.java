package org.pangrova.coordinator.integration.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonConnector;
import org.pangrova.connector.paimon.PaimonConnectorFactory;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real filesystem split/reader shutdown through formal adapter APIs, not Coordinator SQL execution. */
class PaimonConnectorShutdownTest {
    @TempDir Path warehouse;

    @Test
    void shutdown_closesCatalogAndClearsSplitRegistry() throws Exception {
        createTwoFiles();
        var connector = connector();
        var query = new TestConnectorQuery("shutdown-pending");
        var session = query.session("test_catalog", "db");
        var metadata = query.authority().beginMetadata("test_catalog", () -> connector.getMetadata(session));
        var handle = metadata.bindRead(session, metadata.getTableHandle(session, new SchemaTableName("db", "rows")))
                .handle();
        var source = connector.getSplitManager().getSplits(ConnectorTransactionHandle.empty(), session, handle,
                DynamicFilter.ALL, Constraint.EMPTY);
        try {
            while (!source.isFinished()) source.getNextBatch(100);
        } finally {
            source.close();
        }
        assertThat(connector.lifecycleDiagnostics().pendingSplitRegistrations()).isEqualTo(2);
        query.seal();
        connector.shutdown();
        assertThat(connector.lifecycleDiagnostics().pendingSplitRegistrations()).isZero();
        assertThat(connector.lifecycleDiagnostics().activeReaders()).isZero();
        query.authority().cleanupMetadata().requireSuccess();
        query.retire();
    }

    @Test
    void shutdown_isIdempotent() {
        var connector = connector();
        connector.shutdown();
        connector.shutdown();
        assertThat(connector.lifecycleDiagnostics().pendingSplitRegistrations()).isZero();
    }

    @Test
    void activeSdkReaderMustExitBeforeShutdownAndCleanupCanSucceed() throws Exception {
        createTwoFiles();
        var connector = connector();
        var query = new TestConnectorQuery("shutdown-reader");
        var session = query.session("test_catalog", "db");
        var metadata = query.authority().beginMetadata("test_catalog", () -> connector.getMetadata(session));
        var handle = metadata.bindRead(session, metadata.getTableHandle(session, new SchemaTableName("db", "rows")))
                .handle();
        var columns = List.copyOf(metadata.getColumnHandles(session, handle).values());
        org.pangrova.connector.api.metadata.ConnectorSplit split;
        var source = connector.getSplitManager().getSplits(ConnectorTransactionHandle.empty(), session, handle,
                DynamicFilter.ALL, Constraint.EMPTY);
        try {
            split = source.getNextBatch(100).get(0);
        } finally {
            source.close();
        }
        var reader = connector.getPageSourceProvider().createPageSource(ConnectorTransactionHandle.empty(), session,
                split, handle, columns, DynamicFilter.ALL);
        try {
            assertThat(connector.lifecycleDiagnostics().activeReaders()).isOne();
            query.seal();
            assertThatThrownBy(connector::shutdown)
                    .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                    .cause().isInstanceOf(IllegalStateException.class).hasMessageContaining("active readers");
            assertThatThrownBy(() -> query.authority().cleanupMetadata()).hasMessageContaining("not exited");
            assertThat(connector.lifecycleDiagnostics().activeReaders()).isOne();
        } finally {
            reader.close();
        }
        assertThat(connector.lifecycleDiagnostics().activeReaders()).isZero();
        query.authority().cleanupMetadata().requireSuccess();
        connector.shutdown();
        query.retire();
    }

    private PaimonConnector connector() {
        return (PaimonConnector) new PaimonConnectorFactory().create("test_catalog",
                Map.of("warehouse", warehouse.toString()), null);
    }

    private void createTwoFiles() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "rows");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .option("bucket", "-1").option("file.format", "parquet")
                    .option("write-only", "true").build(), false);
            var table = catalog.getTable(id);
            for (int value : List.of(17, 23)) {
                var builder = table.newBatchWriteBuilder();
                try (var write = builder.newWrite(); var commit = builder.newCommit()) {
                    write.write(GenericRow.of(value));
                    commit.commit(write.prepareCommit());
                }
            }
        }
    }
}
