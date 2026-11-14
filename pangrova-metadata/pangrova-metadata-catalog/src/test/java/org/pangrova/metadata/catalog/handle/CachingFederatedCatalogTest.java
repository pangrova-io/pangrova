package org.pangrova.metadata.catalog.handle;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.catalog.federation.FederationConnectorMetadata;

/**
 * Validates F-CHAIN-META-06 fix: federated catalog reads now hit the shared
 * {@link TieredMetadataCache} instead of round-tripping the external metastore
 * 2-3 times per query resolution.
 */
class CachingFederatedCatalogTest {

    private CountingFederatedCatalog backing;
    private TieredMetadataCache cache;
    private CachingFederatedCatalog caching;

    @BeforeEach
    void setUp() {
        backing = new CountingFederatedCatalog();
        cache = new TieredMetadataCache();
        caching = new CachingFederatedCatalog(backing, cache, "fed_cat");
    }

    @Test
    void pullOnlyProviderRejectsPushRegistrationThroughCache() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> caching.subscribeToChanges(change -> {}))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("push change notifications");
    }

    @Test
    void getTableHitsAdapterOnceThenCacheServesSubsequentCalls() {
        // First call: cache miss → adapter is queried once.
        TableMetadata first = caching.getTable("public", "orders");
        assertThat(first).isNotNull();
        assertThat(backing.getTableCalls.get()).isEqualTo(1);

        // Subsequent calls: cache hit → adapter NOT queried again.
        TableMetadata second = caching.getTable("public", "orders");
        TableMetadata third = caching.getTable("public", "orders");
        assertThat(backing.getTableCalls.get()).isEqualTo(1);
        assertThat(second).isSameAs(first);
        assertThat(third).isSameAs(first);
    }

    @Test
    void threeFederationConnectorMethodsShareSingleAdapterFetch() {
        // Simulates FederationConnectorMetadata's three call sites
        // (getTableHandle / getTableMetadata / getColumnHandles) which previously
        // each triggered a separate adapter.getTable() call — F-CHAIN-META-06.
        FederationConnectorMetadata connector = new FederationConnectorMetadata(caching);

        // Trigger the three SPI methods in sequence as the planner would
        var session = StubConnectorSession.INSTANCE;
        var schemaTable = new org.pangrova.connector.api.metadata.table.SchemaTableName("public", "orders");
        var handle = connector.getTableHandle(session, schemaTable);
        connector.getTableMetadata(session, handle);
        connector.getColumnHandles(session, handle);

        // All three were served from a single adapter fetch.
        assertThat(backing.getTableCalls.get()).isEqualTo(1);
    }

    @Test
    void getTableStatisticsHitsAdapterOnceThenCacheServesSubsequentCalls() {
        TableStatistics first = caching.getTableStatistics("public", "orders");
        TableStatistics second = caching.getTableStatistics("public", "orders");
        TableStatistics third = caching.getTableStatistics("public", "orders");

        assertThat(backing.getStatsCalls.get()).isEqualTo(1);
        assertThat(second).isSameAs(first);
        assertThat(third).isSameAs(first);
    }

    @Test
    void invalidateTableInvalidatesBothMetadataAndStats() {
        caching.getTable("public", "orders");
        caching.getTableStatistics("public", "orders");
        assertThat(backing.getTableCalls.get()).isEqualTo(1);
        assertThat(backing.getStatsCalls.get()).isEqualTo(1);

        // CachingFederatedCatalog.invalidate clears both the shared metadata cache
        // and the local stats cache.
        caching.invalidate("public", "orders");

        caching.getTable("public", "orders");
        caching.getTableStatistics("public", "orders");

        assertThat(backing.getTableCalls.get()).isEqualTo(2);
        assertThat(backing.getStatsCalls.get()).isEqualTo(2);
    }

    @Test
    void invalidateAllClearsAllCachedTablesAndStats() {
        caching.getTable("public", "orders");
        caching.getTable("public", "users");
        caching.getTableStatistics("public", "orders");
        assertThat(backing.getTableCalls.get()).isEqualTo(2);
        assertThat(backing.getStatsCalls.get()).isEqualTo(1);

        caching.invalidateAll();

        caching.getTable("public", "orders");
        caching.getTable("public", "users");
        caching.getTableStatistics("public", "orders");

        assertThat(backing.getTableCalls.get()).isEqualTo(4);
        assertThat(backing.getStatsCalls.get()).isEqualTo(2);
    }

    @Test
    void schemaListingsAndCdcArePassedThrough() {
        // List operations are intentionally not cached — they're outside the
        // hot resolution path and inherently dynamic.
        caching.listSchemas();
        caching.listSchemas();
        assertThat(backing.listSchemasCalls.get()).isEqualTo(2);

        caching.listTables("public");
        assertThat(backing.listTablesCalls.get()).isEqualTo(1);

        caching.readChangesSince(Instant.EPOCH);
        assertThat(backing.changesSinceCalls.get()).isEqualTo(1);
    }

    @Test
    void closeClosesUnderlyingDelegate() {
        caching.close();
        assertThat(backing.closed).isTrue();
    }

    // ── Stubs ─────────────────────────────────────────────────────

    static final class CountingFederatedCatalog implements FederatedCatalog {
        final AtomicInteger getTableCalls = new AtomicInteger();
        final AtomicInteger getStatsCalls = new AtomicInteger();
        final AtomicInteger listSchemasCalls = new AtomicInteger();
        final AtomicInteger listTablesCalls = new AtomicInteger();
        final AtomicInteger changesSinceCalls = new AtomicInteger();
        boolean closed;

        @Override
        public List<String> listSchemas() {
            listSchemasCalls.incrementAndGet();
            return List.of("public");
        }

        @Override
        public List<String> listTables(String schema) {
            listTablesCalls.incrementAndGet();
            return List.of("orders", "users");
        }

        @Override
        public TableMetadata getTable(String schema, String table) {
            getTableCalls.incrementAndGet();
            return new TableMetadata(
                    "fed_cat", schema, table,
                    List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                    Map.of(), List.of(), Map.of(), null, null);
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            getStatsCalls.incrementAndGet();
            return new TableStatistics(100L, 1024L, -1, Map.of());
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            changesSinceCalls.incrementAndGet();
            return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(List.of(), java.util.Optional.empty());
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private enum StubConnectorSession implements org.pangrova.connector.api.session.ConnectorSession {
        INSTANCE;

        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public java.util.Optional<String> getCatalog() {
            return java.util.Optional.of("fed_cat");
        }

        @Override
        public java.util.Optional<String> getSchema() {
            return java.util.Optional.of("public");
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of();
        }
    }
}
