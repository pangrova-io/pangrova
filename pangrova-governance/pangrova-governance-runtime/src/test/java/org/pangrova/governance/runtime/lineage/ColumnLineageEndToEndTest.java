package org.pangrova.governance.runtime.lineage;

import org.pangrova.governance.api.lineage.LineageRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test verifying column-level lineage capture:
 * lineage record storage → catalog persistence → query API retrieval.
 */
class ColumnLineageEndToEndTest {

    private LineageCatalog catalog;
    private LineageQueryService queryService;

    @BeforeEach
    void setUp() {
        catalog = new LineageCatalog(100);
        queryService = new LineageQueryService(catalog);
    }

    @Test
    void queryProducesCorrectColumnLevelLineage() {
        // Simulate lineage extracted from: SELECT o.total_price * 0.9 AS discounted, c.name
        // FROM orders o JOIN customers c ON o.customer_id = c.id
        var derivations = List.of(
                new LineageRecord.ColumnDerivationEntry(
                        "discounted",
                        List.of(new LineageRecord.SourceColumnRef("orders", "total_price")),
                        "TRANSFORM"),
                new LineageRecord.ColumnDerivationEntry(
                        "name",
                        List.of(new LineageRecord.SourceColumnRef("customers", "name")),
                        "DIRECT")
        );

        var record = new LineageRecord(
                "query-001",
                "SELECT o.total_price * 0.9 AS discounted, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
                derivations,
                Set.of("orders", "customers"),
                Set.of(),
                System.currentTimeMillis()
        );

        catalog.store(record);

        // Verify retrieval by query ID
        Optional<LineageRecord> retrieved = queryService.getLineageByQueryId("query-001");
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().columnDerivations()).hasSize(2);

        // Verify column derivation for "discounted" output
        var discountedDerivation = retrieved.get().columnDerivations().get(0);
        assertThat(discountedDerivation.outputColumn()).isEqualTo("discounted");
        assertThat(discountedDerivation.derivationType()).isEqualTo("TRANSFORM");
        assertThat(discountedDerivation.sourceColumns())
                .containsExactly(new LineageRecord.SourceColumnRef("orders", "total_price"));

        // Verify column derivation for "name" output
        var nameDerivation = retrieved.get().columnDerivations().get(1);
        assertThat(nameDerivation.outputColumn()).isEqualTo("name");
        assertThat(nameDerivation.derivationType()).isEqualTo("DIRECT");
        assertThat(nameDerivation.sourceColumns())
                .containsExactly(new LineageRecord.SourceColumnRef("customers", "name"));

        // Verify source table queries
        assertThat(retrieved.get().sourceTables()).containsExactlyInAnyOrder("orders", "customers");
        assertThat(retrieved.get().targetTables()).isEmpty();
    }

    @Test
    void lineageQueryBySourceTable() {
        storeSimpleLineage("q1", "SELECT a FROM t1", "t1", "a");
        storeSimpleLineage("q2", "SELECT b FROM t2", "t2", "b");
        storeSimpleLineage("q3", "SELECT c FROM t1", "t1", "c");

        List<LineageRecord> t1Records = queryService.getLineageBySourceTable("t1");
        assertThat(t1Records).hasSize(2);
        assertThat(t1Records).extracting(LineageRecord::queryId)
                .containsExactlyInAnyOrder("q1", "q3");
    }

    @Test
    void lineageQueryBySourceColumn() {
        storeSimpleLineage("q1", "SELECT price FROM products", "products", "price");
        storeSimpleLineage("q2", "SELECT name FROM products", "products", "name");

        List<LineageRecord> priceRecords = queryService.getLineageBySourceColumn("products", "price");
        assertThat(priceRecords).hasSize(1);
        assertThat(priceRecords.get(0).queryId()).isEqualTo("q1");
    }

    @Test
    void aggregateLineageTracksSourceColumns() {
        // Simulate: SELECT category, SUM(amount) AS total FROM sales GROUP BY category
        var derivations = List.of(
                new LineageRecord.ColumnDerivationEntry(
                        "category",
                        List.of(new LineageRecord.SourceColumnRef("sales", "category")),
                        "DIRECT"),
                new LineageRecord.ColumnDerivationEntry(
                        "total",
                        List.of(new LineageRecord.SourceColumnRef("sales", "amount")),
                        "AGGREGATE")
        );

        var record = new LineageRecord(
                "query-agg",
                "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
                derivations,
                Set.of("sales"),
                Set.of(),
                System.currentTimeMillis()
        );

        catalog.store(record);

        Optional<LineageRecord> retrieved = queryService.getLineageByQueryId("query-agg");
        assertThat(retrieved).isPresent();

        var totalDerivation = retrieved.get().columnDerivations().get(1);
        assertThat(totalDerivation.outputColumn()).isEqualTo("total");
        assertThat(totalDerivation.derivationType()).isEqualTo("AGGREGATE");
        assertThat(totalDerivation.sourceColumns())
                .containsExactly(new LineageRecord.SourceColumnRef("sales", "amount"));
    }

    @Test
    void catalogEvictsOldestWhenFull() {
        var smallCatalog = new LineageCatalog(2);
        var smallService = new LineageQueryService(smallCatalog);

        storeInCatalog(smallCatalog, "q1", 1000L);
        storeInCatalog(smallCatalog, "q2", 2000L);
        storeInCatalog(smallCatalog, "q3", 3000L);

        assertThat(smallCatalog.size()).isEqualTo(2);
        assertThat(smallService.getLineageByQueryId("q1")).isEmpty();
        assertThat(smallService.getLineageByQueryId("q2")).isPresent();
        assertThat(smallService.getLineageByQueryId("q3")).isPresent();
    }

    private void storeSimpleLineage(String queryId, String sql, String table, String column) {
        var derivations = List.of(
                new LineageRecord.ColumnDerivationEntry(
                        column,
                        List.of(new LineageRecord.SourceColumnRef(table, column)),
                        "DIRECT")
        );
        catalog.store(new LineageRecord(queryId, sql, derivations,
                Set.of(table), Set.of(), System.currentTimeMillis()));
    }

    private void storeInCatalog(LineageCatalog target, String queryId, long timestamp) {
        target.store(new LineageRecord(queryId, "SELECT 1", List.of(),
                Set.of(), Set.of(), timestamp));
    }
}
