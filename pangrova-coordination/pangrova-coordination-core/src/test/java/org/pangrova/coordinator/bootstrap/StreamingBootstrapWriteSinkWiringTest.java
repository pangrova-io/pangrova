package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.streaming.api.checkpoint.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BUG-D27 fix verification: {@code StreamingBootstrap.createWriteSink} must create
 * a real {@code ConnectorPageSinkCdcWriter} that writes CDC data through the
 * connector PageSink path, not a no-op placeholder that silently discards data.
 */
@DisplayName("D27: CDC write sink wiring — StreamingBootstrap.createWriteSink")
class StreamingBootstrapWriteSinkWiringTest {

    private final List<Page> writtenPages = new CopyOnWriteArrayList<>();
    private final List<String> committedTenantIds = new CopyOnWriteArrayList<>();
    private final AtomicInteger beginInsertCalls = new AtomicInteger();
    private CatalogRegistry catalogRegistry;

    @BeforeEach
    void setUp() {
        catalogRegistry = buildRegistry();
    }

    @Test
    @DisplayName("createWriteSink returns ConnectorPageSinkCdcWriter that writes through PageSink")
    void writeSinkWritesThroughConnectorPageSink() {
        StreamState state = buildStreamState("iceberg.default.cdc_target");
        CdcWriteSink sink = StreamingBootstrap.createWriteSink(state, catalogRegistry);
        assertThat(beginInsertCalls.get()).isZero();

        CdcRow afterRow = CdcRow.of(Map.of("id", 1L, "name", "alice"));
        CdcEnvelope envelope = new CdcEnvelope(
                CdcOperation.INSERT, "cdc_target",
                null, afterRow,
                Instant.now(), 1, Map.of());

        CdcWriteResult result = sink.writeBatch(List.of(envelope));

        assertThat(result.rowsWritten()).isEqualTo(1);
        assertThat(result.targetTable()).isEqualTo("iceberg.default.cdc_target");
        assertThat(writtenPages).as("CDC data must reach the connector PageSink").hasSize(1);
        assertThat(writtenPages.get(0).rows()).hasSize(1);
        assertThat(committedTenantIds).containsExactly("test-tenant");
        assertThat(beginInsertCalls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("createWriteSink routes DELETE through beginDelete + createDeletePageSink")
    void writeSinkSkipsDeleteEnvelopes() {
        StreamState state = buildStreamState("iceberg.default.cdc_target");
        CdcWriteSink sink = StreamingBootstrap.createWriteSink(state, catalogRegistry);
        assertThat(beginInsertCalls.get()).isZero();

        CdcRow beforeRow = CdcRow.of(Map.of("id", 1L, "name", "alice"));
        CdcEnvelope deleteEnvelope = new CdcEnvelope(
                CdcOperation.DELETE, "cdc_target",
                beforeRow, null,
                Instant.now(), 1, Map.of());

        CdcWriteResult result = sink.writeBatch(List.of(deleteEnvelope));

        assertThat(result.rowsWritten()).isEqualTo(1);
        // DELETE routes through the delete page sink (before-image)
        assertThat(writtenPages).hasSize(1);
        assertThat(writtenPages.get(0).rows()).hasSize(1);
        assertThat(beginInsertCalls.get()).isZero();
    }

    @Test
    @DisplayName("createWriteSink handles multi-row batch with operation routing")
    void writeSinkHandlesMultiRowBatch() {
        StreamState state = buildStreamState("iceberg.default.cdc_target");
        CdcWriteSink sink = StreamingBootstrap.createWriteSink(state, catalogRegistry);

        List<CdcEnvelope> batch = new ArrayList<>();
        batch.add(new CdcEnvelope(CdcOperation.INSERT, "cdc_target",
                null, CdcRow.of(Map.of("id", 1L, "name", "alice")),
                Instant.now(), 1, Map.of()));
        batch.add(new CdcEnvelope(CdcOperation.UPDATE, "cdc_target",
                CdcRow.of(Map.of("id", 2L, "name", "bob_old")),
                CdcRow.of(Map.of("id", 2L, "name", "bob_new")),
                Instant.now(), 1, Map.of()));
        batch.add(new CdcEnvelope(CdcOperation.INSERT, "cdc_target",
                null, CdcRow.of(Map.of("id", 3L, "name", "charlie")),
                Instant.now(), 1, Map.of()));

        CdcWriteResult result = sink.writeBatch(batch);

        assertThat(result.rowsWritten()).isEqualTo(3);
        // UPDATE produces delete(before-image) + insert(after-image)
        // Delete phase: 1 row (UPDATE before-image)
        // Insert phase: 3 rows (2 INSERTs + 1 UPDATE after-image)
        assertThat(writtenPages).hasSize(2);
    }

    // ── Test infrastructure ──────────────────────────────────────────

    private StreamState buildStreamState(String targetTable) {
        Instant now = Instant.now();
        return StreamState.builder()
                .streamName("test-stream")
                .tenantId("test-tenant")
                .lifecycleState(StreamLifecycleState.RUNNING)
                .targetTable(targetTable)
                .sourceProperties(Map.of("bootstrap.servers", "localhost:9092", "topic", "test"))
                .sourceFormat("debezium-json")
                .streamProperties(Map.of())
                .creatorIdentity("test-user")
                .coordinatorId("coord-1")
                .epoch(1)
                .schemaVersion(1)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private CatalogRegistry buildRegistry() {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector());
        TestMetadataStore store = new TestMetadataStore();
        CatalogRegistry registry = new CatalogRegistry(store, chf, null, null);
        registry.register(new CatalogDescriptor(
                "iceberg", CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        return registry;
    }

    // ── Minimal test doubles (no mock frameworks) ────────────────────

    private final class TestConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new TestMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider() {
            return new TestPageSinkProvider();
        }
    }

    private final class TestMetadata implements ConnectorMetadata {
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName("default", "cdc_target"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return new TestTableHandle(tableName.schema() + "." + tableName.table());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "cdc_target"),
                    List.of("id", "name"), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of("id", new TestColumnHandle("id"), "name", new TestColumnHandle("name"));
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
            beginInsertCalls.incrementAndGet();
            return new TestOutputHandle(((TestTableHandle) table).key());
        }

        @Override
        public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
            return new TestOutputHandle(((TestTableHandle) table).key());
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            committedTenantIds.add(session.getProperties().get("tenant_id"));
        }
    }

    private record TestTableHandle(String key) implements ConnectorTableHandle {}

    private record TestOutputHandle(String key) implements ConnectorOutputTableHandle {}

    private record TestColumnHandle(String name) implements ColumnHandle { @Override public String columnName() { return name; } }

    private final class TestPageSinkProvider implements ConnectorPageSinkProvider {
        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle txn,
                                                 ConnectorSession session,
                                                 ConnectorOutputTableHandle outputHandle) {
            return new TestPageSink();
        }
    }

    private final class TestPageSink implements ConnectorPageSink {
        @Override
        public void appendPage(Page page) {
            writtenPages.add(page);
        }

        @Override
        public byte[] finishWithMetadata() { return new byte[0]; }

        @Override
        public void abort() {}

        @Override
        public void close() {}
    }

    /**
     * Minimal MetadataStore for CatalogRegistry construction.
     */
    private static final class TestMetadataStore implements MetadataStore {
        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override
        public void saveTable(org.pangrova.metadata.api.object.TableMetadata metadata) {}

        @Override
        public Optional<org.pangrova.metadata.api.object.TableMetadata> loadTable(
                String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) {}

        @Override
        public List<org.pangrova.metadata.api.object.QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            catalogs.removeIf(d -> d.name().equals(descriptor.name()));
            catalogs.add(descriptor);
        }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return catalogs.stream().filter(d -> d.name().equals(name)).findFirst();
        }

        @Override
        public void deleteCatalogDescriptor(String name) {
            catalogs.removeIf(d -> d.name().equals(name));
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.copyOf(catalogs);
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) {}

        @Override
        public void recordSyncHistory(org.pangrova.metadata.api.catalog.SyncResult result) {}

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) {}
    }
}
