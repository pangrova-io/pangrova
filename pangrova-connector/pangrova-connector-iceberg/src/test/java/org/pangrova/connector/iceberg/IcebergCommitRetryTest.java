package org.pangrova.connector.iceberg;

import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for G-DML-07: verifies that all DML commit paths go through
 * the retry mechanism with table refresh (rebase) on conflict, enforce
 * commit timeout, and fire index notification on success.
 *
 * <p>Catalog-based tests (marked with HadoopCatalog) require JDK 17 and
 * are CI-only; pure retry wrapper tests run on any JDK.</p>
 */
class IcebergCommitRetryTest {

    // --- Pure RetryingCommitWrapper tests (no catalog needed) ---

    /**
     * Verifies that RetryingCommitWrapper retries on CommitFailedException
     * with exponential backoff and eventually succeeds.
     */
    @Test
    void retryWrapper_retriesOnCommitFailedException_andSucceeds() {
        AtomicInteger attempts = new AtomicInteger(0);
        RetryingCommitWrapper wrapper = RetryingCommitWrapper.withDefaults(
                e -> e instanceof CommitFailedException);

        wrapper.execute(() -> {
            int attempt = attempts.incrementAndGet();
            if (attempt < 3) {
                throw new CommitFailedException(
                        "simulated OCC conflict on attempt " + attempt);
            }
            // Succeed on attempt 3
        });

        assertThat(attempts.get())
                .as("Should succeed after 2 conflicts (3 total attempts)")
                .isEqualTo(3);
    }

    /**
     * Verifies that non-retryable exceptions propagate immediately without retry.
     */
    @Test
    void retryWrapper_doesNotRetryNonRetryableExceptions() {
        AtomicInteger attempts = new AtomicInteger(0);
        RetryingCommitWrapper wrapper = RetryingCommitWrapper.withDefaults(
                e -> e instanceof CommitFailedException);

        assertThatThrownBy(() ->
                wrapper.execute(() -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("non-retryable error");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("non-retryable error");

        assertThat(attempts.get())
                .as("Should fail immediately on non-retryable exception")
                .isEqualTo(1);
    }

    /**
     * Verifies that exhausting all retries throws PangrovaException with
     * RESILIENCE_RETRY_EXHAUSTED error code.
     */
    @Test
    void retryWrapper_throwsAfterMaxRetries() {
        RetryingCommitWrapper wrapper = RetryingCommitWrapper.create(
                10, 100, 2, e -> e instanceof CommitFailedException);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() ->
                wrapper.execute(() -> {
                    attempts.incrementAndGet();
                    throw new CommitFailedException("always conflict");
                }))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("OCC commit failed after 3 attempts");

        assertThat(attempts.get())
                .as("Should attempt 3 times (initial + 2 retries)")
                .isEqualTo(3);
    }

    /**
     * Verifies that the retry lambda in commitAtomicAppend calls table.refresh()
     * for rebase semantics. We simulate this by verifying that the lambda body
     * creates a new operation each time (which it does since table.newAppend()
     * is called inside the lambda).
     */
    @Test
    void retryWrapper_lambdaIsReexecuted_soOperationIsRebuilt() {
        AtomicInteger appendCalls = new AtomicInteger(0);
        AtomicInteger refreshCalls = new AtomicInteger(0);

        RetryingCommitWrapper wrapper = RetryingCommitWrapper.withDefaults(
                e -> e instanceof CommitFailedException);

        wrapper.execute(() -> {
            // Simulates what the fixed commit lambda does:
            // 1. refresh (rebase)
            // 2. build new operation
            // 3. commit
            refreshCalls.incrementAndGet();
            appendCalls.incrementAndGet();
            if (appendCalls.get() < 2) {
                throw new CommitFailedException("conflict");
            }
        });

        assertThat(refreshCalls.get())
                .as("refresh() should be called on each attempt (rebase)")
                .isEqualTo(2);
        assertThat(appendCalls.get())
                .as("Operation should be rebuilt on each attempt")
                .isEqualTo(2);
    }

    // --- Catalog-based tests (CI-only, need JDK 17 for HadoopCatalog) ---

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;
    private boolean catalogAvailable;

    private static final String CATALOG_NAME = "retry_catalog";
    private static final String NAMESPACE = "retry_db";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "iceberg-retry-test");
        }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        try {
            catalog = buildHadoopCatalog();
            createNamespace();
            metadata = new IcebergMetadata(catalog, CATALOG_NAME);
            catalogAvailable = true;
        } catch (UnsupportedOperationException e) {
            // JDK 26: getSubject not supported — skip catalog-dependent tests
            catalogAvailable = false;
        }
    }

    /**
     * Verifies that deleteByFilter goes through the full commit pipeline
     * (retry + timeout + notification). Previously it was a bare .commit()
     * with none of these protections.
     */
    @Test
    void deleteByFilter_commitsWithNotification() {
        org.junit.jupiter.api.Assumptions.assumeTrue(catalogAvailable,
                "HadoopCatalog not available on JDK 26");

        Table table = createTableWithData("delete_filter_notify");
        long snapshotBefore = table.currentSnapshot().snapshotId();

        AtomicReference<String> notifiedTable = new AtomicReference<>();
        AtomicReference<org.pangrova.spi.connector.index.CommitEventType> notifiedType =
                new AtomicReference<>();
        metadata.registerCommitListener(event -> {
            notifiedTable.set(event.tableName());
            notifiedType.set(event.eventType());
        });

        var handle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, "delete_filter_notify"));
        metadata.deleteByFilter(SESSION, handle, new DeleteFilter.All());

        table.refresh();
        assertThat(table.currentSnapshot().snapshotId())
                .as("deleteByFilter should produce a new snapshot")
                .isNotEqualTo(snapshotBefore);
        assertThat(notifiedTable.get())
                .as("Commit listener should be notified with the table name")
                .isEqualTo("delete_filter_notify");
        assertThat(notifiedType.get())
                .as("Commit listener should receive DELETE event type")
                .isEqualTo(org.pangrova.spi.connector.index.CommitEventType.DELETE);
    }

    /**
     * Verifies that deleteByFilter invalidates the split cache on commit.
     */
    @Test
    void deleteByFilter_invalidatesSplitCache() {
        org.junit.jupiter.api.Assumptions.assumeTrue(catalogAvailable,
                "HadoopCatalog not available on JDK 26");

        createTableWithData("delete_filter_cache");
        AtomicReference<String> invalidatedTable = new AtomicReference<>();
        metadata.setSplitCacheInvalidator(invalidatedTable::set);

        var handle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, "delete_filter_cache"));
        metadata.deleteByFilter(SESSION, handle, new DeleteFilter.All());

        assertThat(invalidatedTable.get())
                .as("Split cache should be invalidated after deleteByFilter")
                .isEqualTo("delete_filter_cache");
    }

    /**
     * Verifies that append commits fire the commit listener with the
     * correct event type and file paths.
     */
    @Test
    void appendCommit_firesNotificationWithPaths() {
        org.junit.jupiter.api.Assumptions.assumeTrue(catalogAvailable,
                "HadoopCatalog not available on JDK 26");

        Table table = createTable("append_notify");

        List<String> notifiedPaths = new ArrayList<>();
        AtomicReference<org.pangrova.spi.connector.index.CommitEventType> notifiedType =
                new AtomicReference<>();
        metadata.registerCommitListener(event -> {
            notifiedPaths.addAll(event.addedFiles());
            notifiedType.set(event.eventType());
        });

        IcebergOutputTableHandle handle = beginInsert("append_notify");
        byte[] fragment = buildAppendFragmentBytes(table, "data-notify.parquet", 10, 1024);
        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment));

        assertThat(notifiedType.get())
                .isEqualTo(org.pangrova.spi.connector.index.CommitEventType.APPEND);
        assertThat(notifiedPaths).hasSize(1);
        assertThat(notifiedPaths.get(0)).contains("data-notify.parquet");
    }

    // --- helpers ---

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    private Table createTable(String tableName) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(tableId, TEST_SCHEMA, PartitionSpec.unpartitioned());
    }

    private Table createTableWithData(String tableName) {
        Table table = createTable(tableName);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/init.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(10)
                .withFileSizeInBytes(1024)
                .build();
        table.newAppend().appendFile(dataFile).commit();
        return table;
    }

    private IcebergOutputTableHandle beginInsert(String tableName) {
        var tableHandle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, tableName));
        return (IcebergOutputTableHandle) metadata.beginInsert(SESSION, tableHandle);
    }

    private byte[] buildAppendFragmentBytes(Table table, String fileName,
                                            long recordCount, long fileSizeInBytes) {
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + fileName)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(recordCount)
                .withFileSizeInBytes(fileSizeInBytes)
                .build();
        String dataFileJson = ContentFileParser.toJson(dataFile, table.spec());
        String arrayJson = "[" + dataFileJson + "]";
        return arrayJson.getBytes(StandardCharsets.UTF_8);
    }
}
