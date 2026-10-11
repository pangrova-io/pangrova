package org.pangrova.testing.e2e.connector;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Authenticated public SQL DDL against the real PostgreSQL Lance catalog and Worker schema compiler. */
@E2ETest
class LanceSqlDdlSessionE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "lance_sql_ddl_session";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(new ConnectorTestConfig(CATALOG, "default", "lance", Map.of(
                "warehouse", tempDir.resolve("lance-ddl-warehouse").toString())));
    }

    @Test
    void schemaCreateTableDescribeAndDropUseTheAuthenticatedTenant() {
        String schema = "ddl_" + UUID.randomUUID().toString().replace("-", "");
        String name = CATALOG + "." + schema + ".docs";
        executeDdl("CREATE SCHEMA " + CATALOG + "." + schema);
        assertThat(executeQuery("SHOW SCHEMAS FROM " + CATALOG))
                .extracting(row -> row.getString("schema_name")).contains(schema);
        executeDdl("CREATE TABLE " + name + " (id BIGINT, title VARCHAR(40))");
        executeDdl("CREATE TABLE IF NOT EXISTS " + name + " (id BIGINT, title VARCHAR(40))");
        assertThat(executeQuery("SHOW TABLES FROM " + CATALOG + "." + schema))
                .extracting(row -> row.getString("table_name")).containsExactly("docs");
        assertThat(executeQuery("DESCRIBE " + name))
                .extracting(row -> row.getString("column_name")).containsExactly("id", "title");
        var session = authenticatedCatalogSession(CATALOG);
        var metadata = metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getMetadata(session);
        assertThat(metadata.getTableHandle(session, new SchemaTableName(schema, "docs"))).isNotNull();
        executeDdl("DROP TABLE " + name);
        executeDdl("DROP TABLE IF EXISTS " + name);
        assertThat(metadata.getTableHandle(session, new SchemaTableName(schema, "docs"))).isNull();
        assertThat(executeQuery("SHOW TABLES FROM " + CATALOG + "." + schema)).isEmpty();
    }

    @Test
    void ctasUsesTheSameAuthorityForSchemaCreationCompilationAndNativeWrite() {
        String schema = "ctas_" + UUID.randomUUID().toString().replace("-", "");
        String source = CATALOG + "." + schema + ".source_rows";
        String target = CATALOG + "." + schema + ".copied_rows";
        executeDdl("CREATE SCHEMA " + CATALOG + "." + schema);
        executeDdl("CREATE TABLE " + source + " (id BIGINT, title VARCHAR(40))");
        executeDml("INSERT INTO " + source + " VALUES (7, 'authenticated')");
        executeDdl("CREATE TABLE " + target + " AS SELECT id, title FROM " + source);
        assertThat(executeQuery("SELECT id, title FROM " + target))
                .singleElement().satisfies(row -> {
                    assertThat(row.getLong("id")).isEqualTo(7L);
                    assertThat(row.getString("title")).isEqualTo("authenticated");
                });
        executeDdl("DROP TABLE " + source);
        executeDdl("DROP TABLE " + target);
    }
}
