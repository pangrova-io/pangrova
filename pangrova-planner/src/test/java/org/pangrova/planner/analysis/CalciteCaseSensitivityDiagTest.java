package org.pangrova.planner.analysis;

import org.pangrova.planner.substrait.SubstraitConverter;
import io.substrait.plan.Plan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Diagnostic test: verifies what column names Calcite produces
 * when caseSensitive=false and schema has lowercase column names.
 */
class CalciteCaseSensitivityDiagTest {

    @Test
    void printColumnNamesFromCalcitePlan() {
        // Scenario 1: flat schema (no sub-schemas)
        CalciteSchema flat = CalciteSchema.createRootSchema(false);
        flat.add("orders", ordersTable());
        testWithPlanner("FLAT", new PangrovaPlanner(flat));

        // Scenario 2: nested schema (catalog.schema.table) like E2E
        CalciteSchema root2 = CalciteSchema.createRootSchema(false);
        var catalogSchema = root2.plus().add("iceberg_catalog",
                new org.apache.calcite.schema.impl.AbstractSchema() {});
        catalogSchema.add("default",
                new org.apache.calcite.schema.impl.AbstractSchema() {
                    @Override
                    protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
                        return java.util.Map.of("orders", ordersTable());
                    }
                });
        PangrovaPlanner nested = new PangrovaPlanner(
                CalciteSchema.from(root2.plus()),
                java.util.List.of("iceberg_catalog", "default"));
        testWithPlanner("NESTED", nested);
    }

    private void testWithPlanner(String label, PangrovaPlanner planner) {
        SubstraitConverter converter = new SubstraitConverter();
        String[] sqls = {
            "SELECT * FROM orders",
            "SELECT o_orderkey FROM orders",
            "SELECT o_orderkey, o_custkey FROM orders ORDER BY o_orderkey LIMIT 10",
            "SELECT COUNT(*) AS total FROM orders"
        };
        for (String sql : sqls) {
            RelNode plan = planner.plan(sql).plan();
            System.err.println("=== [" + label + "] SQL: " + sql);
            System.err.println("  RelNode: " + plan.getClass().getSimpleName());
            for (RelDataTypeField f : plan.getRowType().getFieldList()) {
                System.err.println("  Field: '" + f.getName() + "'");
            }
            Plan substraitPlan = converter.convert(plan);
            System.err.println("  Substrait root names: "
                    + substraitPlan.getRoots().get(0).getNames());
            System.err.println();
        }
    }

    private static AbstractTable ordersTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("o_orderkey", SqlTypeName.BIGINT)
                        .add("o_custkey", SqlTypeName.BIGINT)
                        .add("o_orderstatus", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        };
    }
}
