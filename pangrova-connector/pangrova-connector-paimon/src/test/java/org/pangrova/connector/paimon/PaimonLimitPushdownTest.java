package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LIMIT pushdown is correctly absorbed by the Paimon connector.
 */
class PaimonLimitPushdownTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .build();
        catalog.createTable(Identifier.create("db", "orders"), schema, false);
        metadata = new PaimonMetadata(catalog, "test_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
    }

    @Test
    void applyLimit_returnsHandleWithLimit() {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        assertThat(handle).isNotNull();

        Optional<ConnectorTableHandle> result = metadata.applyLimit(SESSION, handle, 10);

        assertThat(result).isPresent();
        PaimonTableHandle limitHandle = (PaimonTableHandle) result.get();
        assertThat(limitHandle.pushedLimit()).isEqualTo(10);
        assertThat(limitHandle.hasLimit()).isTrue();
    }

    @Test
    void applyLimit_smallerLimitReplacesLarger() {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        Optional<ConnectorTableHandle> first = metadata.applyLimit(SESSION, handle, 100);
        assertThat(first).isPresent();

        Optional<ConnectorTableHandle> second = metadata.applyLimit(SESSION, first.get(), 10);
        assertThat(second).isPresent();
        assertThat(((PaimonTableHandle) second.get()).pushedLimit()).isEqualTo(10);
    }

    @Test
    void applyLimit_largerLimitIsIgnored() {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        Optional<ConnectorTableHandle> first = metadata.applyLimit(SESSION, handle, 10);
        assertThat(first).isPresent();

        Optional<ConnectorTableHandle> second = metadata.applyLimit(SESSION, first.get(), 100);
        assertThat(second).isEmpty();
    }

    @Test
    void applyFilter_emptyConstraintReturnsEmpty() {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        Optional<ConnectorTableHandle> result = metadata.applyFilter(
                SESSION, handle, org.pangrova.connector.spi.metadata.Constraint.EMPTY);

        assertThat(result).isEmpty();
    }

    @Test
    void applyTopN_absorbsLimitPortion() {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        assertThat(handle).isNotNull();

        var columnHandles = metadata.getColumnHandles(SESSION, handle);
        var idColumn = columnHandles.get("id");

        var topN = new org.pangrova.connector.spi.metadata.TopNInfo(
                java.util.List.of(new org.pangrova.connector.spi.metadata.TopNInfo.SortColumn(
                        idColumn, org.pangrova.connector.spi.metadata.TopNInfo.SortOrder.ASC_NULLS_LAST)),
                5);

        Optional<ConnectorTableHandle> result = metadata.applyTopN(SESSION, handle, topN);

        assertThat(result).isPresent();
        PaimonTableHandle topNHandle = (PaimonTableHandle) result.get();
        assertThat(topNHandle.pushedLimit()).isEqualTo(5);
        assertThat(topNHandle.hasLimit()).isTrue();
    }
}
