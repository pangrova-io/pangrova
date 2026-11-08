package org.pangrova.testing.testkit;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.pangrova.types.value.TypedRow;

/**
 * Generates TPC-H schema test data for integration and validation testing.
 *
 * <p>Supports all 8 TPC-H tables (lineitem, orders, customer, part, supplier,
 * partsupp, nation, region) with deterministic random data seeded for reproducibility.
 */
public final class TestDataGenerator {

    /** Supported column types for test data generation. */
    public enum ColumnType {
        STRING, LONG, DOUBLE, DATE, BOOLEAN
    }

    /** Definition of a single column: name and type. */
    public record ColumnDef(String name, ColumnType type) {}

    /** Schema definition as an ordered list of column definitions. */
    public record TableSchema(List<ColumnDef> columns) {
        public TableSchema {
            columns = List.copyOf(columns);
        }
    }

    /**
     * Column-specific value pools for STRING type generation.
     * Matches TPC-H domain semantics so that test queries using
     * literal filters (e.g. {@code o_orderstatus = 'F'}) produce
     * meaningful results.
     */
    private static final Map<String, String[]> STRING_VALUE_OVERRIDES = Map.ofEntries(
            Map.entry("o_orderstatus", new String[]{"F", "O", "P"}),
            Map.entry("o_orderpriority", new String[]{
                    "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}),
            Map.entry("l_returnflag", new String[]{"A", "N", "R"}),
            Map.entry("l_linestatus", new String[]{"F", "O"}),
            Map.entry("l_shipinstruct", new String[]{
                    "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}),
            Map.entry("l_shipmode", new String[]{
                    "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}),
            Map.entry("c_mktsegment", new String[]{
                    "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}),
            Map.entry("n_name", new String[]{
                    "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "CHINA",
                    "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA",
                    "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN",
                    "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "ROMANIA",
                    "RUSSIA", "SAUDI ARABIA", "UNITED KINGDOM", "UNITED STATES", "VIETNAM"}),
            Map.entry("r_name", new String[]{
                    "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"})
    );

    /**
     * Primary key columns that must generate unique sequential values (1-based).
     * TPC-H spec requires these to be unique identifiers within their table.
     * Foreign key columns referencing these PKs remain random within the PK range.
     */
    private static final Set<String> PRIMARY_KEY_COLUMNS = Set.of(
            "o_orderkey", "c_custkey", "p_partkey", "s_suppkey",
            "n_nationkey", "r_regionkey"
    );

    /**
     * Columns eligible for NULL injection. In TPC-H all columns are NOT NULL,
     * but we inject NULLs into comment columns to exercise NULL-handling paths
     * in the engine. All other columns stay NOT NULL per TPC-H spec.
     */
    private static final Set<String> NULLABLE_COLUMNS = Set.of(
            "o_comment", "l_comment", "c_comment", "p_comment",
            "s_comment", "ps_comment", "n_comment", "r_comment"
    );

    private static final Map<String, Integer> LONG_RANGE_OVERRIDES = Map.ofEntries(
            Map.entry("n_regionkey", 5),
            Map.entry("r_regionkey", 5),
            Map.entry("n_nationkey", 25),
            Map.entry("c_nationkey", 25),
            Map.entry("s_nationkey", 25),
            // JOIN key columns — constrain ranges so cross-table JOINs produce matches
            Map.entry("o_orderkey", 100),
            Map.entry("l_orderkey", 100),
            Map.entry("o_custkey", 50),
            Map.entry("c_custkey", 50),
            Map.entry("l_partkey", 200),
            Map.entry("p_partkey", 200),
            Map.entry("l_suppkey", 10),
            Map.entry("s_suppkey", 10),
            Map.entry("ps_partkey", 200),
            Map.entry("ps_suppkey", 10),
            // Miscellaneous small-range columns
            Map.entry("o_shippriority", 5),
            Map.entry("l_linenumber", 7),
            Map.entry("p_size", 50),
            Map.entry("ps_availqty", 1000)
    );

    private static final Map<String, TableSchema> SCHEMAS = buildSchemas();

    private final Random random;

    /**
     * Create a generator with the given random seed for reproducibility.
     *
     * @param seed the random seed
     */
    public TestDataGenerator(long seed) {
        this.random = new Random(seed);
    }

    /** Create a generator with the default seed (42). */
    public TestDataGenerator() {
        this(42L);
    }

    /**
     * Generate test rows for a TPC-H table.
     *
     * @param tableName the TPC-H table name (case-insensitive); must not be null
     * @param rows      the number of rows to generate; must be positive
     * @return list of generated typed rows
     * @throws IllegalArgumentException if tableName is unknown or rows is not positive
     */
    public List<TypedRow> generateTable(String tableName, int rows) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        if (rows <= 0) {
            throw new IllegalArgumentException("rows must be positive, got: " + rows);
        }
        TableSchema schema = SCHEMAS.get(tableName.toLowerCase());
        if (schema == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return generateRows(schema, rows);
    }

    /**
     * Return an immutable copy of all supported TPC-H table schemas.
     *
     * @return map of table name to schema definition
     */
    public static Map<String, TableSchema> getSchemas() {
        return Map.copyOf(SCHEMAS);
    }

    private List<TypedRow> generateRows(TableSchema schema, int rows) {
        List<TypedRow> result = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            result.add(generateRow(schema, i));
        }
        return result;
    }

    private TypedRow generateRow(TableSchema schema, int rowIndex) {
        TypedRow.Builder builder = TypedRow.builder();
        for (ColumnDef col : schema.columns()) {
            builder.putRaw(col.name(), generateValue(col.name(), col.type(), rowIndex));
        }
        return builder.build();
    }

    private Object generateValue(String columnName, ColumnType type, int rowIndex) {
        if (NULLABLE_COLUMNS.contains(columnName) && shouldBeNull(rowIndex)) {
            return null;
        }
        return switch (type) {
            case STRING -> {
                String[] pool = STRING_VALUE_OVERRIDES.get(columnName);
                yield pool != null ? pool[random.nextInt(pool.length)] : generateString(rowIndex);
            }
            case LONG -> generateLong(columnName, rowIndex);
            case DOUBLE -> Math.round(random.nextDouble() * 100_000.0) / 100.0;
            case DATE -> LocalDate.of(1992, 1, 1).plusDays(random.nextInt(2557));
            case BOOLEAN -> random.nextBoolean();
        };
    }

    private long generateLong(String columnName, int rowIndex) {
        int range = LONG_RANGE_OVERRIDES.getOrDefault(columnName, 1_000_000);
        if (PRIMARY_KEY_COLUMNS.contains(columnName)) {
            return (long) ((rowIndex % range) + 1);
        }
        // Use 1-based generation for columns with explicit range overrides
        // (FK columns referencing 1-based PKs). This aligns FK range [1, range]
        // with PK range [1, range], ensuring JOIN matches are always possible.
        if (LONG_RANGE_OVERRIDES.containsKey(columnName)) {
            return (long) (random.nextInt(range) + 1);
        }
        return (long) random.nextInt(range);
    }

    private boolean shouldBeNull(int rowIndex) {
        return rowIndex % 50 == 49;
    }

    private String generateString(int rowIndex) {
        if (rowIndex % 100 == 99) {
            return "special'chars\"test\\n";
        }
        return "val_" + random.nextInt(10_000);
    }

    private static Map<String, TableSchema> buildSchemas() {
        Map<String, TableSchema> schemas = new HashMap<>();
        schemas.put("lineitem", buildLineitemSchema());
        schemas.put("orders", buildOrdersSchema());
        schemas.put("customer", buildCustomerSchema());
        schemas.put("part", buildPartSchema());
        schemas.put("supplier", buildSupplierSchema());
        schemas.put("partsupp", buildPartsuppSchema());
        schemas.put("nation", buildNationSchema());
        schemas.put("region", buildRegionSchema());
        return Map.copyOf(schemas);
    }

    private static TableSchema buildLineitemSchema() {
        return new TableSchema(List.of(
            new ColumnDef("l_orderkey", ColumnType.LONG),
            new ColumnDef("l_partkey", ColumnType.LONG),
            new ColumnDef("l_suppkey", ColumnType.LONG),
            new ColumnDef("l_linenumber", ColumnType.LONG),
            new ColumnDef("l_quantity", ColumnType.DOUBLE),
            new ColumnDef("l_extendedprice", ColumnType.DOUBLE),
            new ColumnDef("l_discount", ColumnType.DOUBLE),
            new ColumnDef("l_tax", ColumnType.DOUBLE),
            new ColumnDef("l_returnflag", ColumnType.STRING),
            new ColumnDef("l_linestatus", ColumnType.STRING),
            new ColumnDef("l_shipdate", ColumnType.DATE),
            new ColumnDef("l_commitdate", ColumnType.DATE),
            new ColumnDef("l_receiptdate", ColumnType.DATE),
            new ColumnDef("l_shipinstruct", ColumnType.STRING),
            new ColumnDef("l_shipmode", ColumnType.STRING),
            new ColumnDef("l_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildOrdersSchema() {
        return new TableSchema(List.of(
            new ColumnDef("o_orderkey", ColumnType.LONG),
            new ColumnDef("o_custkey", ColumnType.LONG),
            new ColumnDef("o_orderstatus", ColumnType.STRING),
            new ColumnDef("o_totalprice", ColumnType.DOUBLE),
            new ColumnDef("o_orderdate", ColumnType.DATE),
            new ColumnDef("o_orderpriority", ColumnType.STRING),
            new ColumnDef("o_clerk", ColumnType.STRING),
            new ColumnDef("o_shippriority", ColumnType.LONG),
            new ColumnDef("o_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildCustomerSchema() {
        return new TableSchema(List.of(
            new ColumnDef("c_custkey", ColumnType.LONG),
            new ColumnDef("c_name", ColumnType.STRING),
            new ColumnDef("c_address", ColumnType.STRING),
            new ColumnDef("c_nationkey", ColumnType.LONG),
            new ColumnDef("c_phone", ColumnType.STRING),
            new ColumnDef("c_acctbal", ColumnType.DOUBLE),
            new ColumnDef("c_mktsegment", ColumnType.STRING),
            new ColumnDef("c_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildPartSchema() {
        return new TableSchema(List.of(
            new ColumnDef("p_partkey", ColumnType.LONG),
            new ColumnDef("p_name", ColumnType.STRING),
            new ColumnDef("p_mfgr", ColumnType.STRING),
            new ColumnDef("p_brand", ColumnType.STRING),
            new ColumnDef("p_type", ColumnType.STRING),
            new ColumnDef("p_size", ColumnType.LONG),
            new ColumnDef("p_container", ColumnType.STRING),
            new ColumnDef("p_retailprice", ColumnType.DOUBLE),
            new ColumnDef("p_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildSupplierSchema() {
        return new TableSchema(List.of(
            new ColumnDef("s_suppkey", ColumnType.LONG),
            new ColumnDef("s_name", ColumnType.STRING),
            new ColumnDef("s_address", ColumnType.STRING),
            new ColumnDef("s_nationkey", ColumnType.LONG),
            new ColumnDef("s_phone", ColumnType.STRING),
            new ColumnDef("s_acctbal", ColumnType.DOUBLE),
            new ColumnDef("s_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildPartsuppSchema() {
        return new TableSchema(List.of(
            new ColumnDef("ps_partkey", ColumnType.LONG),
            new ColumnDef("ps_suppkey", ColumnType.LONG),
            new ColumnDef("ps_availqty", ColumnType.LONG),
            new ColumnDef("ps_supplycost", ColumnType.DOUBLE),
            new ColumnDef("ps_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildNationSchema() {
        return new TableSchema(List.of(
            new ColumnDef("n_nationkey", ColumnType.LONG),
            new ColumnDef("n_name", ColumnType.STRING),
            new ColumnDef("n_regionkey", ColumnType.LONG),
            new ColumnDef("n_comment", ColumnType.STRING)
        ));
    }

    private static TableSchema buildRegionSchema() {
        return new TableSchema(List.of(
            new ColumnDef("r_regionkey", ColumnType.LONG),
            new ColumnDef("r_name", ColumnType.STRING),
            new ColumnDef("r_comment", ColumnType.STRING)
        ));
    }
}
