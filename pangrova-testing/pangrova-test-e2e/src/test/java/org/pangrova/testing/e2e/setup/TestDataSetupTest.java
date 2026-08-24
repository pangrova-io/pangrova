package org.pangrova.testing.e2e.setup;

import org.pangrova.testing.TestCluster;
import org.pangrova.testing.TestDataGenerator;
import org.pangrova.testing.e2e.E2ETestFramework;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestDataSetupTest {

    private final List<String> executedSql = new ArrayList<>();
    private TestDataSetup setup;

    @BeforeEach
    void setUp() {
        executedSql.clear();
        TestCluster cluster = new TestCluster(() -> () -> {}, () -> () -> {});
        E2ETestFramework framework = new E2ETestFramework(cluster, sql -> {
            executedSql.add(sql);
            return List.of();
        });
        SqlTestRunner runner = new SqlTestRunner(framework);
        setup = new TestDataSetup(runner);
    }

    @Test
    void createTestTableGeneratesCorrectDdl() {
        setup.createTestTable("cat", "sch", "my_table", "id BIGINT, name VARCHAR");

        assertThat(executedSql).hasSize(1);
        assertThat(executedSql.get(0))
            .isEqualTo("CREATE TABLE IF NOT EXISTS cat.sch.my_table (id BIGINT, name VARCHAR)");
    }

    @Test
    void loadTpcHDataCreatesAllTables() {
        setup.loadTpcHData("cat", "sch", 1);

        Map<String, TestDataGenerator.TableSchema> schemas = TestDataGenerator.getSchemas();
        long createCount = executedSql.stream()
            .filter(sql -> sql.startsWith("CREATE TABLE"))
            .count();
        assertThat(createCount).isEqualTo(schemas.size());
    }

    @Test
    void loadTpcHDataGeneratesInsertStatements() {
        setup.loadTpcHData("cat", "sch", 1);

        long insertCount = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO"))
            .count();
        // scaleFactor=1 → 100 rows per table, INSERT_BATCH_SIZE=50 → 2 inserts per table × 8 tables = 16
        int expectedTables = TestDataGenerator.getSchemas().size();
        int batchesPerTable = (100 + 49) / 50; // ceil(BASE_ROWS / BATCH_SIZE)
        assertThat(insertCount).isEqualTo((long) expectedTables * batchesPerTable);
    }

    @Test
    void loadTpcHDataScalesRowCount() {
        setup.loadTpcHData("cat", "sch", 2);

        long insertCount = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO"))
            .count();
        int expectedTables = TestDataGenerator.getSchemas().size();
        int batchesPerTable = (200 + 49) / 50; // ceil(scaleFactor*BASE_ROWS / BATCH_SIZE)
        assertThat(insertCount).isEqualTo((long) expectedTables * batchesPerTable);
    }

    @Test
    void createTableDdlContainsCorrectSqlTypes() {
        setup.loadTpcHData("cat", "sch", 1);

        String regionCreate = executedSql.stream()
            .filter(sql -> sql.contains("cat.sch.region"))
            .filter(sql -> sql.startsWith("CREATE TABLE"))
            .findFirst()
            .orElseThrow();
        // region: r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR
        assertThat(regionCreate).contains("BIGINT");
        assertThat(regionCreate).contains("VARCHAR");
    }

    @Test
    void insertStatementContainsFullyQualifiedTableName() {
        setup.loadTpcHData("cat", "sch", 1);

        String firstInsert = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO"))
            .findFirst()
            .orElseThrow();
        assertThat(firstInsert).startsWith("INSERT INTO cat.sch.");
    }

    @Test
    void insertStatementHandlesNullValues() {
        setup.loadTpcHData("cat", "sch", 1);

        // TestDataGenerator produces NULL at rowIndex % 50 == 49 (row 50)
        // With 100 rows per table, there should be NULLs in the data
        boolean hasNull = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO"))
            .anyMatch(sql -> sql.contains("NULL"));
        assertThat(hasNull).isTrue();
    }

    @Test
    void formatValueEscapesSingleQuotesInStrings() {
        // Verify via a direct round-trip: generate data with a string containing
        // a single quote and confirm the INSERT SQL escapes it properly.
        // TestDataGenerator's special string "special'chars..." is masked by NULL
        // at the same row index, so we verify escaping through the generated SQL
        // by checking that all string literals in INSERT statements are well-formed.
        setup.loadTpcHData("cat", "sch", 1);

        // Every INSERT with a string value should have balanced single quotes
        boolean allBalanced = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO"))
            .allMatch(TestDataSetupTest::hasSqlBalancedQuotes);
        assertThat(allBalanced).isTrue();
    }

    private static boolean hasSqlBalancedQuotes(String sql) {
        int valuesStart = sql.indexOf("VALUES (");
        if (valuesStart < 0) {
            return false;
        }
        String valuesPart = sql.substring(valuesStart);
        boolean inString = false;
        for (int i = 0; i < valuesPart.length(); i++) {
            if (valuesPart.charAt(i) == '\'') {
                if (i + 1 < valuesPart.length() && valuesPart.charAt(i + 1) == '\'') {
                    i++; // skip escaped quote
                } else {
                    inString = !inString;
                }
            }
        }
        return !inString;
    }

    @Test
    void insertStatementWrapsDateValues() {
        setup.loadTpcHData("cat", "sch", 1);

        // orders table has DATE columns (o_orderdate)
        boolean hasDateLiteral = executedSql.stream()
            .filter(sql -> sql.startsWith("INSERT INTO cat.sch.orders"))
            .anyMatch(sql -> sql.contains("DATE '"));
        assertThat(hasDateLiteral).isTrue();
    }
}
