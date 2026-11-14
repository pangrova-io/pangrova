package org.pangrova.planner.lineage;

import org.pangrova.planner.api.lineage.ColumnDerivation;

import org.pangrova.planner.analysis.PangrovaPlanner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Plans actual SQL against official Calcite JDBC metadata from real PostgreSQL tables. */
class CalciteColumnLineageIntegrationTest {
    private static final GenericContainer<?> DATABASE = new GenericContainer<>("postgres:16-alpine")
            .withExposedPorts(5432).withEnv("POSTGRES_DB", "lineage")
            .withEnv("POSTGRES_USER", "lineage").withEnv("POSTGRES_PASSWORD", "lineage")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    private static PGSimpleDataSource dataSource;

    @BeforeAll
    static void initializeActualSchema() throws Exception {
        DATABASE.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://" + DATABASE.getHost() + ":"
                + DATABASE.getMappedPort(5432) + "/lineage");
        dataSource.setUser("lineage");
        dataSource.setPassword("lineage");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE src (id INTEGER, amount INTEGER, category VARCHAR(40))");
            statement.execute("CREATE TABLE dst (id INTEGER, amount INTEGER, category VARCHAR(40))");
            statement.execute("CREATE TABLE array_src (id INTEGER, tags INTEGER[])");
        }
    }

    @AfterAll
    static void closeDatabase() {
        DATABASE.close();
    }

    private List<ColumnDerivation> plan(String sql) {
        var root = CalciteSchema.createRootSchema(false);
        root.plus().add("lake", JdbcSchema.create(root.plus(), "lake", dataSource, null, "public"));
        var result = new PangrovaPlanner(root, List.of("lake")).plan(sql).toPlannedQuery();
        return result.columnLineage();
    }

    private static ColumnDerivation.SourceColumn source(String table, String column) {
        return new ColumnDerivation.SourceColumn("lake." + table, column);
    }

    @Test
    void nestedAggregateRetainsItsKindThroughProjectionAndSort() {
        var columns = plan("SELECT total + 1 AS adjusted FROM "
                + "(SELECT SUM(amount) AS total FROM lake.src) s ORDER BY adjusted");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.AGGREGATE);
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("src", "amount"));
    }

    @Test
    void transformedGroupingKeyRetainsItsExpressionClassification() {
        var columns = plan("SELECT amount + 1 AS key_value FROM lake.src GROUP BY amount + 1");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("src", "amount"));
    }

    @Test
    void setOperationCombinesBothRealInputOrigins() {
        var columns = plan("SELECT amount AS value FROM lake.src UNION SELECT amount FROM lake.dst");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns())
                .containsExactly(source("dst", "amount"), source("src", "amount"));
    }

    @Test
    void outerJoinNullGenerationMarksTheRealRightValueDerived() {
        var columns = plan("SELECT s.id, d.amount FROM lake.src s LEFT JOIN lake.dst d ON s.id = d.id");
        assertThat(columns).hasSize(2);
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.DIRECT);
        assertThat(columns.get(1).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("dst", "amount"));
    }

    @Test
    void windowIncludesOperandsPartitionAndOrderingFromItsActualInput() {
        var columns = plan("SELECT SUM(amount) OVER (PARTITION BY category ORDER BY id "
                + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running FROM lake.src");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.AGGREGATE);
        assertThat(columns.get(0).sourceColumns())
                .containsExactly(source("src", "amount"), source("src", "category"), source("src", "id"));
    }

    @Test
    void actualConstantExpressionHasKnownEmptyOrigins() {
        var columns = plan("SELECT 7 AS constant_value");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns()).isEmpty();
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
    }

    @Test
    void insertMapsActualTargetFieldsAndPreservesExpressionDependencies() {
        var columns = plan("INSERT INTO lake.dst (id, amount, category) "
                + "SELECT id, amount + 1, category FROM lake.src");
        assertThat(columns.stream().map(ColumnDerivation::outputColumn)).containsExactly("id", "amount", "category");
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("src", "id"));
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("src", "amount"));
        assertThat(columns.get(1).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
        assertThat(columns.get(2).sourceColumns()).containsExactly(source("src", "category"));
    }

    @Test
    void updateIncludesValueAndPreservedCopyOnWriteRowsRatherThanRowCount() {
        var columns = plan("UPDATE lake.dst SET amount = amount + 1 WHERE id = 7");
        assertThat(columns.stream().map(ColumnDerivation::outputColumn)).containsExactly("id", "amount", "category");
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("dst", "id"));
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("dst", "amount"));
        assertThat(columns.get(1).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
        assertThat(columns.get(2).sourceColumns()).containsExactly(source("dst", "category"));
    }

    @Test
    void deleteMapsEachActualRetainedTargetColumn() {
        var columns = plan("DELETE FROM lake.dst WHERE id = 7");
        assertThat(columns.stream().map(ColumnDerivation::outputColumn)).containsExactly("id", "amount", "category");
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("dst", "id"));
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("dst", "amount"));
        assertThat(columns.get(2).sourceColumns()).containsExactly(source("dst", "category"));
    }

    @Test
    void mergeMapsSourceInsertionAndUpdatedOrPreservedTargetValues() {
        var columns = plan("MERGE INTO lake.dst d USING lake.src s ON d.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount "
                + "WHEN NOT MATCHED THEN INSERT (id, amount, category) VALUES (s.id, s.amount, s.category)");
        assertThat(columns.stream().map(ColumnDerivation::outputColumn)).containsExactly("id", "amount", "category");
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("dst", "id"), source("src", "id"));
        assertThat(columns.get(1).sourceColumns())
                .containsExactly(source("dst", "amount"), source("src", "amount"));
        assertThat(columns.get(2).sourceColumns())
                .containsExactly(source("dst", "category"), source("src", "category"));
    }
    @Test
    void correlatedUnnestUsesItsActualLeftInputsForEachArrayValue() {
        var columns = plan("SELECT u.value FROM lake.src s "
                + "CROSS JOIN UNNEST(ARRAY[s.amount, s.id]) AS u(value)");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns())
                .containsExactly(source("src", "amount"), source("src", "id"));
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
    }

    @Test
    void correlatedOrdinalityDependsOnTheActualCollectionInputs() {
        var columns = plan("SELECT u.position FROM lake.src s "
                + "CROSS JOIN UNNEST(ARRAY[s.amount, s.id]) WITH ORDINALITY AS u(value, position)");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns())
                .containsExactly(source("src", "amount"), source("src", "id"));
    }

    @Test
    void standaloneLiteralUnnestHasKnownEmptyOrigins() {
        var columns = plan("SELECT u.value FROM UNNEST(ARRAY[1, 2, 3]) AS u(value)");
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns()).isEmpty();
        assertThat(columns.get(0).expression()).isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
    }

    @Test
    void correlatedColumnUnnestKeepsItsActualArraySource() {
        var columns = plan("SELECT a.id, u.value, u.position FROM lake.array_src a "
                + "CROSS JOIN UNNEST(a.tags) WITH ORDINALITY AS u(value, position) "
                + "ORDER BY a.id, u.position");
        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("array_src", "id"));
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("array_src", "tags"));
        assertThat(columns.get(2).sourceColumns()).containsExactly(source("array_src", "tags"));
    }

    @Test
    void scalarCorrelatedAggregateRetainsActualValueOriginsAndAggregateKind() {
        var columns = plan("SELECT s.id, (SELECT SUM(d.amount) FROM lake.dst d WHERE d.id = s.id) "
                + "AS total FROM lake.src s");
        assertThat(columns).hasSize(2);
        assertThat(columns.get(0).sourceColumns()).containsExactly(source("src", "id"));
        assertThat(columns.get(1).sourceColumns()).containsExactly(source("dst", "amount"));
        assertThat(columns.get(1).expression()).isEqualTo(ColumnDerivation.DerivationType.AGGREGATE);
    }

}
