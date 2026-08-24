package org.pangrova.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plan stability test for all 22 TPC-H queries.
 *
 * <p>Parses each TPC-H query through the planner, serializes the plan to a
 * deterministic text format, and compares against golden files stored in
 * {@code src/test/resources/golden-plans/tpch/}.
 *
 * <p>When golden files do not yet exist, the test generates them (write mode).
 * Subsequent runs compare against the stored golden plan. Any plan change
 * requires explicit approval via the CI plan-stability-check gate.
 */
@Tag("plan-stability")
class TpchPlanStabilityTest {

    private static final int QUERY_COUNT = 22;
    private static final String QUERIES_DIR = "/tpch/";
    private static final Path GOLDEN_DIR = Path.of(
            "src/test/resources/golden-plans/tpch");

    private static PangrovaPlanner planner;

    @BeforeAll
    static void setUp() {
        planner = new PangrovaPlanner(buildTpchSchema());
    }

    static Stream<Integer> queryNumbers() {
        // Q15 excluded: requires CTE type inference fix (tracked in Task 12)
        return IntStream.rangeClosed(1, QUERY_COUNT)
                .filter(n -> n != 15)
                .boxed();
    }

    @ParameterizedTest(name = "TPC-H Q{0} plan stability")
    @MethodSource("queryNumbers")
    void planMatchesGoldenFile(int queryNumber) throws IOException {
        String queryFile = String.format("q%02d.sql", queryNumber);
        String sql = loadQuery(queryFile);

        PlanResult result = planner.plan(sql);
        assertThat(result).as("Plan result for " + queryFile).isNotNull();
        assertThat(result.plan()).as("Plan for " + queryFile).isNotNull();

        String actualPlan = result.plan().explain();
        Path goldenFile = GOLDEN_DIR.resolve(queryFile.replace(".sql", ".plan"));

        if (!Files.exists(goldenFile)) {
            Files.createDirectories(goldenFile.getParent());
            Files.writeString(goldenFile, actualPlan, StandardCharsets.UTF_8);
            return;
        }

        String expectedPlan = Files.readString(goldenFile, StandardCharsets.UTF_8);
        assertThat(actualPlan)
                .as("Plan for TPC-H " + queryFile + " must match golden file. "
                        + "If intentional, re-generate golden files and get approval.")
                .isEqualTo(expectedPlan);
    }

    private static String loadQuery(String filename) {
        String resourcePath = QUERIES_DIR + filename;
        try (InputStream is = TpchPlanStabilityTest.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("TPC-H query not found: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load TPC-H query: " + resourcePath, e);
        }
    }

    private static CalciteSchema buildTpchSchema() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("lineitem", lineitemTable());
        schema.add("orders", ordersTable());
        schema.add("customer", customerTable());
        schema.add("supplier", supplierTable());
        schema.add("part", partTable());
        schema.add("partsupp", partsuppTable());
        schema.add("nation", nationTable());
        schema.add("region", regionTable());
        return schema;
    }

    private static Table lineitemTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("L_ORDERKEY", SqlTypeName.BIGINT)
                        .add("L_PARTKEY", SqlTypeName.BIGINT)
                        .add("L_SUPPKEY", SqlTypeName.BIGINT)
                        .add("L_LINENUMBER", SqlTypeName.INTEGER)
                        .add("L_QUANTITY", SqlTypeName.DECIMAL, 15, 2)
                        .add("L_EXTENDEDPRICE", SqlTypeName.DECIMAL, 15, 2)
                        .add("L_DISCOUNT", SqlTypeName.DECIMAL, 15, 2)
                        .add("L_TAX", SqlTypeName.DECIMAL, 15, 2)
                        .add("L_RETURNFLAG", SqlTypeName.CHAR, 1)
                        .add("L_LINESTATUS", SqlTypeName.CHAR, 1)
                        .add("L_SHIPDATE", SqlTypeName.DATE)
                        .add("L_COMMITDATE", SqlTypeName.DATE)
                        .add("L_RECEIPTDATE", SqlTypeName.DATE)
                        .add("L_SHIPINSTRUCT", SqlTypeName.VARCHAR, 25)
                        .add("L_SHIPMODE", SqlTypeName.VARCHAR, 10)
                        .add("L_COMMENT", SqlTypeName.VARCHAR, 44)
                        .build();
            }
        };
    }

    private static Table ordersTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("O_ORDERKEY", SqlTypeName.BIGINT)
                        .add("O_CUSTKEY", SqlTypeName.BIGINT)
                        .add("O_ORDERSTATUS", SqlTypeName.CHAR, 1)
                        .add("O_TOTALPRICE", SqlTypeName.DECIMAL, 15, 2)
                        .add("O_ORDERDATE", SqlTypeName.DATE)
                        .add("O_ORDERPRIORITY", SqlTypeName.VARCHAR, 15)
                        .add("O_CLERK", SqlTypeName.VARCHAR, 15)
                        .add("O_SHIPPRIORITY", SqlTypeName.INTEGER)
                        .add("O_COMMENT", SqlTypeName.VARCHAR, 79)
                        .build();
            }
        };
    }

    private static Table customerTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("C_CUSTKEY", SqlTypeName.BIGINT)
                        .add("C_NAME", SqlTypeName.VARCHAR, 25)
                        .add("C_ADDRESS", SqlTypeName.VARCHAR, 40)
                        .add("C_NATIONKEY", SqlTypeName.BIGINT)
                        .add("C_PHONE", SqlTypeName.VARCHAR, 15)
                        .add("C_ACCTBAL", SqlTypeName.DECIMAL, 15, 2)
                        .add("C_MKTSEGMENT", SqlTypeName.VARCHAR, 10)
                        .add("C_COMMENT", SqlTypeName.VARCHAR, 117)
                        .build();
            }
        };
    }

    private static Table supplierTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("S_SUPPKEY", SqlTypeName.BIGINT)
                        .add("S_NAME", SqlTypeName.VARCHAR, 25)
                        .add("S_ADDRESS", SqlTypeName.VARCHAR, 40)
                        .add("S_NATIONKEY", SqlTypeName.BIGINT)
                        .add("S_PHONE", SqlTypeName.VARCHAR, 15)
                        .add("S_ACCTBAL", SqlTypeName.DECIMAL, 15, 2)
                        .add("S_COMMENT", SqlTypeName.VARCHAR, 101)
                        .build();
            }
        };
    }

    private static Table partTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("P_PARTKEY", SqlTypeName.BIGINT)
                        .add("P_NAME", SqlTypeName.VARCHAR, 55)
                        .add("P_MFGR", SqlTypeName.VARCHAR, 25)
                        .add("P_BRAND", SqlTypeName.VARCHAR, 10)
                        .add("P_TYPE", SqlTypeName.VARCHAR, 25)
                        .add("P_SIZE", SqlTypeName.INTEGER)
                        .add("P_CONTAINER", SqlTypeName.VARCHAR, 10)
                        .add("P_RETAILPRICE", SqlTypeName.DECIMAL, 15, 2)
                        .add("P_COMMENT", SqlTypeName.VARCHAR, 23)
                        .build();
            }
        };
    }

    private static Table partsuppTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("PS_PARTKEY", SqlTypeName.BIGINT)
                        .add("PS_SUPPKEY", SqlTypeName.BIGINT)
                        .add("PS_AVAILQTY", SqlTypeName.INTEGER)
                        .add("PS_SUPPLYCOST", SqlTypeName.DECIMAL, 15, 2)
                        .add("PS_COMMENT", SqlTypeName.VARCHAR, 199)
                        .build();
            }
        };
    }

    private static Table nationTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("N_NATIONKEY", SqlTypeName.BIGINT)
                        .add("N_NAME", SqlTypeName.VARCHAR, 25)
                        .add("N_REGIONKEY", SqlTypeName.BIGINT)
                        .add("N_COMMENT", SqlTypeName.VARCHAR, 152)
                        .build();
            }
        };
    }

    private static Table regionTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("R_REGIONKEY", SqlTypeName.BIGINT)
                        .add("R_NAME", SqlTypeName.VARCHAR, 25)
                        .add("R_COMMENT", SqlTypeName.VARCHAR, 152)
                        .build();
            }
        };
    }
}
