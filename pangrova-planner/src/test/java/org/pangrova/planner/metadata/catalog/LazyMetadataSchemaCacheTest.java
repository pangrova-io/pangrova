package org.pangrova.planner.metadata.catalog;

import java.util.Optional;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that Calcite can use the same table view without a backing catalog.
 */
class LazyMetadataSchemaCacheTest {

    @Test
    void tableAccessorsShareOneViewWithoutACatalog() {
        var schema = new LazyMetadataSchema(null, "cat", "sch", new PlanningConnectorSession("metadata-user", Optional.empty(), Optional.empty(),
                Map.of("tenant_id", "metadata-tenant"), Optional.empty()));

        Map<String, Table> first = schema.getTableMap();
        Map<String, Table> second = schema.getTableMap();

        assertThat(first).isSameAs(second);
        assertThat(schema.getTableNames()).isEmpty();
        assertThat(schema.getTable("missing")).isNull();
    }

    @Test
    void ddlInvalidationKeepsTheViewUsableWithoutACatalog() {
        var schema = new LazyMetadataSchema(null, "cat", "sch", new PlanningConnectorSession("metadata-user", Optional.empty(), Optional.empty(),
                Map.of("tenant_id", "metadata-tenant"), Optional.empty()));

        Map<String, Table> before = schema.getTableMap();
        MetadataSchemaBuilder.invalidateTableCaches();
        Map<String, Table> after = schema.getTableMap();

        assertThat(before).isEmpty();
        assertThat(after).isEmpty();
        assertThat(after).isSameAs(before);
    }
}
