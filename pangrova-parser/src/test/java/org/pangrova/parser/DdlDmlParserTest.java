package org.pangrova.parser;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.statement.*;
import org.pangrova.parser.ast.statement.dml.*;
import org.pangrova.parser.ast.statement.maintenance.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive DDL/DML parser tests covering all statement types,
 * error recovery, and boundary conditions.
 * Addresses review finding P2-1: Parser test coverage gap.
 */
class DdlDmlParserTest {

    private final SqlParser parser = new SqlParser();

    // ── CREATE TABLE ──

    @Nested
    class CreateTableTests {
        @Test
        void simpleCreateTable() {
            Statement stmt = parser.parse(
                    "CREATE TABLE t1 (id INT, name VARCHAR)");
            assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        }

        @Test
        void createTableWithPartition() {
            Statement stmt = parser.parse(
                    "CREATE TABLE t1 (id INT, ts TIMESTAMP) PARTITIONED BY (ts)");
            assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        }

        @Test
        void createTableWithProperties() {
            Statement stmt = parser.parse(
                    "CREATE TABLE t1 (id INT) WITH ('format' = 'parquet')");
            assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        }

        @Test
        void createTableWithComment() {
            Statement stmt = parser.parse(
                    "CREATE TABLE t1 (id INT) COMMENT 'test table'");
            assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        }
    }

    // ── DROP TABLE ──

    @Nested
    class DropTableTests {
        @Test
        void dropTable() {
            Statement stmt = parser.parse("DROP TABLE t1");
            assertThat(stmt).isInstanceOf(DropTableStatement.class);
        }

        @Test
        void dropTableIfExists() {
            Statement stmt = parser.parse("DROP TABLE IF EXISTS t1");
            assertThat(stmt).isInstanceOf(DropTableStatement.class);
        }

        @Test
        void dropQualifiedTable() {
            Statement stmt = parser.parse("DROP TABLE catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(DropTableStatement.class);
        }
    }

    // ── ALTER TABLE ──

    @Nested
    class AlterTableTests {
        @Test
        void alterTableAddColumn() {
            Statement stmt = parser.parse(
                    "ALTER TABLE t1 ADD COLUMN age INT");
            assertThat(stmt).isInstanceOf(AlterTableStatement.class);
        }

        @Test
        void alterTableDropColumn() {
            Statement stmt = parser.parse(
                    "ALTER TABLE t1 DROP COLUMN age");
            assertThat(stmt).isInstanceOf(AlterTableStatement.class);
        }

        @Test
        void alterTableRenameColumn() {
            Statement stmt = parser.parse(
                    "ALTER TABLE t1 RENAME COLUMN old_name TO new_name");
            assertThat(stmt).isInstanceOf(AlterTableStatement.class);
        }

        @Test
        void alterTableAdopt() {
            Statement stmt = parser.parse("ALTER TABLE catalog1.schema1.t1 ADOPT");

            assertThat(stmt).isInstanceOfSatisfying(AlterTableStatement.Adopt.class,
                    adopt -> assertThat(adopt.tableName().parts())
                            .containsExactly("catalog1", "schema1", "t1"));
        }

        @Test
        void alterTableAdoptWithMaintenancePolicy() {
            Statement stmt = parser.parse("ALTER TABLE catalog1.schema1.t1 ADOPT "
                    + "WITH (maintenance_operations='COMPACT,EXPIRE_SNAPSHOTS', retain_last=5)");

            assertThat(stmt).isInstanceOfSatisfying(AlterTableStatement.Adopt.class,
                    adopt -> {
                        assertThat(adopt.tableName().parts())
                                .containsExactly("catalog1", "schema1", "t1");
                        assertThat(adopt.properties())
                                .containsEntry("maintenance_operations", "COMPACT,EXPIRE_SNAPSHOTS")
                                .containsEntry("retain_last", "5");
                    });
        }

        @Test
        void alterTableUnlink() {
            Statement stmt = parser.parse("ALTER TABLE catalog1.schema1.t1 UNLINK");

            assertThat(stmt).isInstanceOfSatisfying(AlterTableStatement.Unlink.class,
                    unlink -> assertThat(unlink.tableName().parts())
                            .containsExactly("catalog1", "schema1", "t1"));
        }
    }

    // ── INSERT ──

    @Nested
    class InsertTests {
        @Test
        void insertIntoValues() {
            Statement stmt = parser.parse(
                    "INSERT INTO t1 VALUES (1, 'hello')");
            assertThat(stmt).isInstanceOf(InsertStatement.class);
        }

        @Test
        void insertIntoSelect() {
            Statement stmt = parser.parse(
                    "INSERT INTO t1 SELECT * FROM t2");
            assertThat(stmt).isInstanceOf(InsertSelectStatement.class);
        }

        @Test
        void insertOverwrite() {
            Statement stmt = parser.parse(
                    "INSERT OVERWRITE t1 VALUES (1, 'hello')");
            assertThat(stmt).isInstanceOf(InsertOverwriteStatement.class);
        }
    }

    // ── DELETE ──

    @Nested
    class DeleteTests {
        @Test
        void deleteWithWhere() {
            Statement stmt = parser.parse("DELETE FROM t1 WHERE id = 1");
            assertThat(stmt).isInstanceOf(DeleteStatement.class);
        }

        @Test
        void deleteAll() {
            Statement stmt = parser.parse("DELETE FROM t1");
            assertThat(stmt).isInstanceOf(DeleteStatement.class);
        }
    }

    // ── UPDATE ──

    @Nested
    class UpdateTests {
        @Test
        void updateWithWhere() {
            Statement stmt = parser.parse(
                    "UPDATE t1 SET name = 'new' WHERE id = 1");
            assertThat(stmt).isInstanceOf(UpdateStatement.class);
        }

        @Test
        void updateMultipleColumns() {
            Statement stmt = parser.parse(
                    "UPDATE t1 SET name = 'new', age = 30 WHERE id = 1");
            assertThat(stmt).isInstanceOf(UpdateStatement.class);
        }
    }

    // ── MERGE ──

    @Nested
    class MergeTests {
        @Test
        void mergeInto() {
            Statement stmt = parser.parse(
                    "MERGE INTO t1 USING t2 ON t1.id = t2.id "
                            + "WHEN MATCHED THEN UPDATE SET name = t2.name "
                            + "WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.name)");
            assertThat(stmt).isInstanceOf(MergeStatement.class);
        }
    }

    // ── SCHEMA ──

    @Nested
    class SchemaTests {
        @Test
        void createSchema() {
            Statement stmt = parser.parse("CREATE SCHEMA my_schema");
            assertThat(stmt).isInstanceOf(CreateSchemaStatement.class);
        }

        @Test
        void showSchemas() {
            Statement stmt = parser.parse("SHOW SCHEMAS");
            assertThat(stmt).isInstanceOf(ShowSchemasStatement.class);
        }

        @Test
        void showTables() {
            Statement stmt = parser.parse("SHOW TABLES");
            assertThat(stmt).isInstanceOf(ShowTablesStatement.class);
        }

        @Test
        void showTableControlsWithoutScope() {
            Statement stmt = parser.parse("SHOW TABLE CONTROLS");
            assertThat(stmt).isInstanceOf(ShowTableControlsStatement.class);
            assertThat(((ShowTableControlsStatement) stmt).scope()).isEmpty();
        }

        @Test
        void showTableControlsWithCatalogSchemaScope() {
            Statement stmt = parser.parse("SHOW TABLE CONTROLS FROM cat.db");
            assertThat(((ShowTableControlsStatement) stmt).scope())
                    .containsExactly("cat", "db");
        }

        @Test
        void showTableControlsWithSingleTableScope() {
            Statement stmt = parser.parse("SHOW TABLE CONTROLS IN cat.db.orders");
            assertThat(((ShowTableControlsStatement) stmt).scope())
                    .containsExactly("cat", "db", "orders");
        }

        @Test
        void describeTable() {
            Statement stmt = parser.parse("DESCRIBE t1");
            assertThat(stmt).isInstanceOf(DescribeTableStatement.class);
        }
    }

    // ── CATALOG ──

    @Nested
    class CatalogTests {
        @Test
        void createCatalog() {
            Statement stmt = parser.parse(
                    "CREATE CATALOG my_catalog WITH ('type' = 'iceberg')");
            assertThat(stmt).isInstanceOf(CreateCatalogStatement.class);
        }

        @Test
        void dropCatalog() {
            Statement stmt = parser.parse("DROP CATALOG my_catalog");
            assertThat(stmt).isInstanceOf(DropCatalogStatement.class);
        }

        @Test
        void showCatalogs() {
            Statement stmt = parser.parse("SHOW CATALOGS");
            assertThat(stmt).isInstanceOf(ShowCatalogsStatement.class);
        }

        @Test
        void describeCatalog() {
            Statement stmt = parser.parse("DESCRIBE CATALOG my_catalog");
            assertThat(stmt).isInstanceOf(DescribeCatalogStatement.class);
        }
    }

    // ── INDEX ──

    @Nested
    class IndexTests {
        @Test
        void createIndex() {
            Statement stmt = parser.parse(
                    "CREATE INDEX idx1 ON t1 (name) USING BLOOM_FILTER");
            assertThat(stmt).isInstanceOf(CreateIndexStatement.class);
        }

        @Test
        void dropIndex() {
            Statement stmt = parser.parse("DROP INDEX idx1 ON t1");
            assertThat(stmt).isInstanceOf(DropIndexStatement.class);
        }

        @Test
        void showIndexes() {
            Statement stmt = parser.parse("SHOW INDEXES ON t1");
            assertThat(stmt).isInstanceOf(ShowIndexesStatement.class);
        }
    }

    // ── MAINTENANCE ──

    @Nested
    class MaintenanceTests {
        @Test
        void compactTable() {
            Statement stmt = parser.parse("COMPACT catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(CompactStatement.class);
        }

        @Test
        void optimizeTable() {
            Statement stmt = parser.parse("OPTIMIZE catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(OptimizeStatement.class);
        }

        @Test
        void vacuumTable() {
            Statement stmt = parser.parse("VACUUM catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(VacuumStatement.class);
        }

        @Test
        void analyzeTable() {
            Statement stmt = parser.parse("ANALYZE TABLE catalog1.schema1.t1");
            assertThat(stmt)
                    .isInstanceOf(org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement.class);
        }

        @Test
        void analyzeTableComputeStatistics() {
            var stmt = (org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement)
                    parser.parse("ANALYZE TABLE catalog1.schema1.t1 COMPUTE STATISTICS");
            assertThat(stmt.tableName().parts()).containsExactly("catalog1", "schema1", "t1");
        }
    }

    // ── EXPLAIN ──

    @Nested
    class ExplainTests {
        @Test
        void explainSelect() {
            Statement stmt = parser.parse("EXPLAIN SELECT * FROM t1");
            assertThat(stmt).isInstanceOf(ExplainStatement.class);
        }
    }

    // ── CTE ──

    @Nested
    class CteTests {
        @Test
        void withClause() {
            Statement stmt = parser.parse(
                    "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte");
            assertThat(stmt).isInstanceOf(CteQuery.class);
        }

        @Test
        void multipleCtes() {
            Statement stmt = parser.parse(
                    "WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id) "
                            + "SELECT * FROM a UNION ALL SELECT * FROM b");
            assertThat(stmt).isInstanceOf(CteQuery.class);
        }
    }

    // ── Error recovery ──

    @Nested
    class ErrorRecoveryTests {
        @Test
        void missingTableNameInCreate() {
            assertThatThrownBy(() -> parser.parse("CREATE TABLE"))
                    .isInstanceOf(ParseException.class)
                    .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_SYNTAX_ERROR);
        }

        @Test
        void missingValuesInInsert() {
            assertThatThrownBy(() -> parser.parse("INSERT INTO t1"))
                    .isInstanceOf(ParseException.class);
        }

        @Test
        void unclosedParenthesis() {
            assertThatThrownBy(() -> parser.parse("SELECT (1 + 2"))
                    .isInstanceOf(ParseException.class);
        }

        @Test
        void nullInputThrows() {
            assertThatThrownBy(() -> parser.parse(null))
                    .isInstanceOf(ParseException.class)
                    .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
        }

        @Test
        void exceedMaxLengthThrows() {
            String longSql = "SELECT " + "1+".repeat(600_000) + "1";
            assertThatThrownBy(() -> parser.parse(longSql))
                    .isInstanceOf(ParseException.class);
        }
    }

    // ── Boundary: depth limit ──

    @Nested
    class DepthLimitTests {
        @Test
        void deeplyNestedExpressionWithinLimit() {
            // 50 levels of nesting — should be within default 256 limit
            String nested = "(" .repeat(50) + "1" + ")".repeat(50);
            Statement stmt = parser.parse("SELECT " + nested);
            assertThat(stmt).isInstanceOf(Query.class);
        }

        @Test
        void customDepthLimitEnforced() {
            SqlParser strictParser = new SqlParser(1_048_576, 10);
            String nested = "(" .repeat(20) + "1" + ")".repeat(20);
            assertThatThrownBy(() -> strictParser.parse("SELECT " + nested))
                    .isInstanceOf(ParseException.class);
        }
    }

    // ── SET operations ──

    @Nested
    class SetOperationTests {
        @Test
        void unionAll() {
            Statement stmt = parser.parse(
                    "SELECT 1 UNION ALL SELECT 2");
            assertThat(stmt).isInstanceOf(SetQuery.class);
        }

        @Test
        void unionDistinct() {
            Statement stmt = parser.parse(
                    "SELECT 1 UNION SELECT 2");
            assertThat(stmt).isInstanceOf(SetQuery.class);
        }

        @Test
        void intersect() {
            Statement stmt = parser.parse(
                    "SELECT 1 INTERSECT SELECT 1");
            assertThat(stmt).isInstanceOf(SetQuery.class);
        }

        @Test
        void except() {
            Statement stmt = parser.parse(
                    "SELECT 1 EXCEPT SELECT 2");
            assertThat(stmt).isInstanceOf(SetQuery.class);
        }
    }

    // ── Clustering ──

    @Nested
    class ClusteringTests {
        @Test
        void setClusteringOrder() {
            Statement stmt = parser.parse(
                    "ALTER TABLE catalog1.schema1.t1 SET CLUSTERING ORDER (col1, col2)");
            assertThat(stmt).isInstanceOf(SetClusteringOrderStatement.class);
        }

        @Test
        void reclusterTable() {
            Statement stmt = parser.parse("RECLUSTER TABLE catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(ReclusterTableStatement.class);
        }

        @Test
        void showClusteringStatus() {
            Statement stmt = parser.parse("SHOW CLUSTERING STATUS FOR TABLE catalog1.schema1.t1");
            assertThat(stmt).isInstanceOf(ShowClusteringStatusStatement.class);
        }
    }

    // ── CREATE VIEW ──

    @Nested
    class CreateViewTests {
        @Test
        void createViewPreservesWhitespace() {
            String sql = "CREATE VIEW v1 AS SELECT  a,  b   FROM  t1  WHERE  x > 1";
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(CreateViewStatement.class);
            CreateViewStatement view = (CreateViewStatement) stmt;
            assertThat(view.viewSql()).isEqualTo("SELECT  a,  b   FROM  t1  WHERE  x > 1");
        }

        @Test
        void createViewPreservesNewlinesAndIndentation() {
            String sql = "CREATE VIEW v2 AS\nSELECT\n    col1,\n    col2\nFROM\n    orders\nWHERE\n    status = 'active'";
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(CreateViewStatement.class);
            CreateViewStatement view = (CreateViewStatement) stmt;
            assertThat(view.viewSql()).isEqualTo(
                    "SELECT\n    col1,\n    col2\nFROM\n    orders\nWHERE\n    status = 'active'");
        }

        @Test
        void createViewIfNotExists() {
            String sql = "CREATE VIEW IF NOT EXISTS myview AS SELECT id FROM users";
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(CreateViewStatement.class);
            CreateViewStatement view = (CreateViewStatement) stmt;
            assertThat(view.ifNotExists()).isTrue();
            assertThat(view.viewSql()).isEqualTo("SELECT id FROM users");
        }

        @Test
        void createViewWithComplexQuery() {
            String sql = "CREATE VIEW report AS SELECT   a.id,   b.name FROM   accounts a   JOIN   users b ON a.user_id = b.id WHERE   a.active = true";
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(CreateViewStatement.class);
            CreateViewStatement view = (CreateViewStatement) stmt;
            assertThat(view.viewSql()).isEqualTo(
                    "SELECT   a.id,   b.name FROM   accounts a   JOIN   users b ON a.user_id = b.id WHERE   a.active = true");
        }

        @Test
        void createViewPreservesComments() {
            String sql = "CREATE VIEW v3 AS SELECT /* important columns */ a, b FROM t1 -- filter active\nWHERE active = true";
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(CreateViewStatement.class);
            CreateViewStatement view = (CreateViewStatement) stmt;
            assertThat(view.viewSql()).isEqualTo(
                    "SELECT /* important columns */ a, b FROM t1 -- filter active\nWHERE active = true");
        }
    }
}
