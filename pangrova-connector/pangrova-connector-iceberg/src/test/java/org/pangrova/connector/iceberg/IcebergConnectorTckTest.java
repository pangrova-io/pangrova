package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.testing.testkit.AbstractConnectorTckTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg connector TCK — verifies that {@link IcebergMetadata} honors the
 * {@link ConnectorMetadata} contract as defined by {@link AbstractConnectorTckTest}.
 */
class IcebergConnectorTckTest extends AbstractConnectorTckTest {

    private static final String SCHEMA = "tck_db";
    private static final String TABLE = "tck_table";
    private static final String CATALOG_NAME = "tck_catalog";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "tck"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    @Override
    protected void setUpConnector() {
        HadoopTestGuard.assumeHadoopCompatible();
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        catalog = org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(SCHEMA));
        }
        catalog.createTable(TableIdentifier.of(SCHEMA, TABLE), TEST_SCHEMA);
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
    }

    @Override
    protected ConnectorMetadata createMetadata() {
        return metadata;
    }

    @Override
    protected ConnectorSession createSession() {
        return SESSION;
    }

    @Override
    protected String testSchemaName() {
        return SCHEMA;
    }

    @Override
    protected String testTableName() {
        return TABLE;
    }
}
