package org.pangrova.planner.property.substrait;

import org.pangrova.planner.substrait.SubstraitConverter;
import io.substrait.plan.Plan;
import net.jqwik.api.*;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 3: RelNode → Substrait mapping completeness.
 *
 * <p><b>Validates: Requirements 1.4, 9.8</b></p>
 *
 * <p>For any valid Calcite RelNode operator type, {@link SubstraitConverter}
 * should produce a valid Substrait Plan without losing operator semantics.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 3: RelNode → Substrait mapping completeness")
class SubstraitConverterPropertyTest {

    private final SubstraitConverter converter = new SubstraitConverter();

    @Property(tries = 10)
    void relNodeConversionProducesValidSubstraitPlan(
            @ForAll("relNodeSqlStatements") String sql) {
        PangrovaPlanner planner = buildPlanner();
        RelNode relNode = planner.plan(sql).plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).as("Plan for [%s] must not be null", sql).isNotNull();
        assertThat(plan.getRoots()).as("Plan roots must not be empty").isNotEmpty();
        assertThat(plan.getRoots().get(0).getInput())
                .as("Root input must not be null").isNotNull();
    }

    // ── Provider ──

    @Provide
    Arbitrary<String> relNodeSqlStatements() {
        return Arbitraries.oneOf(
                projectSql(),
                filterSql(),
                valuesSql(),
                tableScanSql(),
                sortSql(),
                joinSql(),
                aggregateSql(),
                unionSql(),
                intersectSql(),
                exceptSql(),
                windowSql()
        );
    }

    // ── SQL generators per RelNode type ──

    private Arbitrary<String> projectSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1",
                "SELECT ID, NAME FROM T1",
                "SELECT ID + 1 FROM T1",
                "SELECT ID * 2, NAME FROM T1",
                "SELECT ID - 1 FROM T1",
                "SELECT ID, ID + ID FROM T1",
                "SELECT NAME FROM T1",
                "SELECT ID FROM T2",
                "SELECT VAL FROM T2",
                "SELECT ID, VAL FROM T2"
        );
    }

    private Arbitrary<String> filterSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1 WHERE ID > 0",
                "SELECT NAME FROM T1 WHERE ID < 100",
                "SELECT ID FROM T1 WHERE ID = 42",
                "SELECT ID FROM T1 WHERE ID > 10",
                "SELECT NAME FROM T1 WHERE ID = 1",
                "SELECT ID FROM T2 WHERE ID > 0",
                "SELECT VAL FROM T2 WHERE ID < 50",
                "SELECT ID FROM T2 WHERE ID = 7",
                "SELECT ID FROM T1 WHERE ID > 5",
                "SELECT NAME FROM T1 WHERE ID < 10"
        );
    }

    private Arbitrary<String> valuesSql() {
        return Arbitraries.of(
                "SELECT 1",
                "SELECT 1, 2, 3",
                "SELECT 'hello'",
                "SELECT TRUE, FALSE",
                "SELECT 42",
                "SELECT 1 + 2",
                "SELECT 10 * 3",
                "SELECT 'abc'",
                "SELECT TRUE",
                "SELECT 100"
        );
    }

    private Arbitrary<String> tableScanSql() {
        return Arbitraries.of(
                "SELECT * FROM T1",
                "SELECT * FROM T2",
                "SELECT ID, NAME FROM T1",
                "SELECT VAL FROM T2",
                "SELECT ID FROM T1",
                "SELECT NAME FROM T1",
                "SELECT ID FROM T2",
                "SELECT ID, VAL FROM T2",
                "SELECT NAME, ID FROM T1",
                "SELECT VAL, ID FROM T2"
        );
    }

    private Arbitrary<String> sortSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1 ORDER BY ID",
                "SELECT ID, NAME FROM T1 ORDER BY NAME",
                "SELECT ID FROM T1 ORDER BY ID DESC",
                "SELECT ID FROM T1 ORDER BY ID LIMIT 10",
                "SELECT NAME FROM T1 ORDER BY NAME",
                "SELECT ID FROM T1 ORDER BY ID LIMIT 5",
                "SELECT ID FROM T2 ORDER BY ID",
                "SELECT VAL FROM T2 ORDER BY VAL",
                "SELECT ID FROM T2 ORDER BY ID DESC",
                "SELECT ID FROM T1 ORDER BY ID LIMIT 1"
        );
    }

    private Arbitrary<String> joinSql() {
        return Arbitraries.of(
                "SELECT a.ID FROM T1 a INNER JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID FROM T1 a LEFT JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID FROM T1 a RIGHT JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID FROM T1 a FULL JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID FROM T1 a CROSS JOIN T2 b",
                "SELECT a.NAME FROM T1 a INNER JOIN T2 b ON a.ID = b.ID",
                "SELECT b.VAL FROM T1 a INNER JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID, b.VAL FROM T1 a LEFT JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID, a.NAME FROM T1 a RIGHT JOIN T2 b ON a.ID = b.ID",
                "SELECT a.ID, b.ID FROM T1 a FULL JOIN T2 b ON a.ID = b.ID"
        );
    }

    private Arbitrary<String> aggregateSql() {
        return Arbitraries.of(
                "SELECT COUNT(*) FROM T1",
                "SELECT NAME, COUNT(*) FROM T1 GROUP BY NAME",
                "SELECT SUM(ID) FROM T1",
                "SELECT NAME, MAX(ID) FROM T1 GROUP BY NAME",
                "SELECT MIN(ID) FROM T1",
                "SELECT NAME, MIN(ID) FROM T1 GROUP BY NAME",
                "SELECT COUNT(*) FROM T2",
                "SELECT SUM(ID) FROM T2",
                "SELECT MAX(ID) FROM T2",
                "SELECT MIN(ID) FROM T2"
        );
    }

    private Arbitrary<String> unionSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1 UNION ALL SELECT ID FROM T2",
                "SELECT ID FROM T1 UNION SELECT ID FROM T2",
                "SELECT NAME FROM T1 UNION ALL SELECT VAL FROM T2",
                "SELECT NAME FROM T1 UNION SELECT VAL FROM T2",
                "SELECT 1 UNION ALL SELECT 2",
                "SELECT 1 UNION SELECT 2",
                "SELECT ID FROM T1 UNION ALL SELECT ID FROM T1",
                "SELECT ID FROM T2 UNION ALL SELECT ID FROM T2",
                "SELECT ID FROM T2 UNION SELECT ID FROM T2",
                "SELECT NAME FROM T1 UNION ALL SELECT NAME FROM T1"
        );
    }

    private Arbitrary<String> intersectSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1 INTERSECT SELECT ID FROM T2",
                "SELECT NAME FROM T1 INTERSECT SELECT VAL FROM T2",
                "SELECT ID FROM T1 INTERSECT SELECT ID FROM T1",
                "SELECT 1 INTERSECT SELECT 1",
                "SELECT ID FROM T2 INTERSECT SELECT ID FROM T2"
        );
    }

    private Arbitrary<String> exceptSql() {
        return Arbitraries.of(
                "SELECT ID FROM T1 EXCEPT SELECT ID FROM T2",
                "SELECT NAME FROM T1 EXCEPT SELECT VAL FROM T2",
                "SELECT ID FROM T1 EXCEPT SELECT ID FROM T1",
                "SELECT 1 EXCEPT SELECT 2",
                "SELECT ID FROM T2 EXCEPT SELECT ID FROM T2"
        );
    }

    private Arbitrary<String> windowSql() {
        return Arbitraries.of(
                "SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM T1",
                "SELECT ID, RANK() OVER (ORDER BY ID) FROM T1",
                "SELECT ID, SUM(ID) OVER (ORDER BY ID) FROM T1",
                "SELECT ID, DENSE_RANK() OVER (PARTITION BY NAME ORDER BY ID) FROM T1",
                "SELECT ID, ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY ID) FROM T1",
                "SELECT ID, COUNT(*) OVER (ORDER BY ID) FROM T1",
                "SELECT ID, MIN(ID) OVER (ORDER BY ID) FROM T1",
                "SELECT ID, MAX(ID) OVER (ORDER BY ID) FROM T1",
                "SELECT ID, SUM(ID) OVER (PARTITION BY NAME ORDER BY ID) FROM T1",
                "SELECT ID, RANK() OVER (PARTITION BY NAME ORDER BY ID) FROM T1"
        );
    }

    // ── Schema setup ──

    private static PangrovaPlanner buildPlanner() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("T1", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("ID", SqlTypeName.INTEGER)
                        .add("NAME", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        });
        rootSchema.add("T2", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("ID", SqlTypeName.INTEGER)
                        .add("VAL", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        });
        return new PangrovaPlanner(rootSchema);
    }
}
