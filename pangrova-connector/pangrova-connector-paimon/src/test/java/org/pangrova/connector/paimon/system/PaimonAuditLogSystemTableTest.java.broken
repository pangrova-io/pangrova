package org.pangrova.connector.paimon.system;

import org.pangrova.spi.connector.data.TypedRow;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PaimonAuditLogSystemTable} verifying that non-VARCHAR
 * column types (INT, BIGINT, BOOLEAN, DOUBLE, DECIMAL, TIMESTAMP) are
 * correctly formatted as strings instead of causing ClassCastException.
 *
 * <p>Validates fix for F-PAIMON-05.</p>
 */
class PaimonAuditLogSystemTableTest {

    @TempDir
    static Path warehouse;

    private static Table multiTypeTable;

    @BeforeAll
    static void setUp() throws Exception {
        Catalog catalog = buildCatalog();
        catalog.createDatabase("audit_db", false);

        // Create a table with diverse column types to exercise type-safe formatting
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("amount", DataTypes.BIGINT())
                .column("price", DataTypes.DECIMAL(10, 2))
                .column("active", DataTypes.BOOLEAN())
                .column("score", DataTypes.DOUBLE())
                .column("ratio", DataTypes.FLOAT())
                .column("name", DataTypes.STRING())
                .column("created_at", DataTypes.TIMESTAMP(3))
                .primaryKey("id")
                .option("bucket", "1")
                .option("changelog-producer", "input")
                .build();
        catalog.createTable(Identifier.create("audit_db", "multi_type"), schema, false);
        multiTypeTable = catalog.getTable(Identifier.create("audit_db", "multi_type"));
        writeMultiTypeData(multiTypeTable);
    }

    @Test
    void auditLog_handlesIntegerColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        // INT column should be formatted as a numeric string, not throw ClassCastException
        String idValue = first.getString("id");
        assertThat(idValue).isEqualTo("42");
    }

    @Test
    void auditLog_handlesBigIntColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String amountValue = first.getString("amount");
        assertThat(amountValue).isEqualTo("100000");
    }

    @Test
    void auditLog_handlesDecimalColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String priceValue = first.getString("price");
        assertThat(priceValue).isEqualTo("99.99");
    }

    @Test
    void auditLog_handlesBooleanColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String activeValue = first.getString("active");
        assertThat(activeValue).isEqualTo("true");
    }

    @Test
    void auditLog_handlesDoubleColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String scoreValue = first.getString("score");
        assertThat(scoreValue).isEqualTo("3.14");
    }

    @Test
    void auditLog_handlesFloatColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String ratioValue = first.getString("ratio");
        // Float precision may vary; just verify it parses as a number
        assertThat(Float.parseFloat(ratioValue)).isCloseTo(1.5f, org.assertj.core.data.Offset.offset(0.01f));
    }

    @Test
    void auditLog_handlesVarcharColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String nameValue = first.getString("name");
        assertThat(nameValue).isEqualTo("test_item");
    }

    @Test
    void auditLog_handlesTimestampColumn() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String tsValue = first.getString("created_at");
        // Verify it's a valid ISO timestamp string
        assertThat(tsValue).contains("2024-01-15");
        assertThat(tsValue).contains("10:30:00");
    }

    @Test
    void auditLog_handlesNullValues() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        // Second row has null fields
        assertThat(rows.size()).isGreaterThanOrEqualTo(2);
        TypedRow second = rows.get(1);
        assertThat(second.getString("name")).isNull();
        assertThat(second.getString("score")).isNull();
    }

    @Test
    void auditLog_includesRowKind() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<TypedRow> rows = auditLog.rows();

        assertThat(rows).isNotEmpty();
        TypedRow first = rows.get(0);
        String rowKind = first.getString("row_kind");
        assertThat(rowKind).isIn("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE");
    }

    @Test
    void auditLog_columnsIncludeRowKindAndAllFields() {
        PaimonAuditLogSystemTable auditLog = new PaimonAuditLogSystemTable(multiTypeTable);
        List<String> columns = auditLog.columns();

        assertThat(columns).containsExactly(
                "row_kind", "id", "amount", "price", "active",
                "score", "ratio", "name", "created_at");
    }

    @Test
    void formatField_handlesAllPrimitiveTypes() {
        // Unit-test the static formatField method directly with a GenericRow
        GenericRow row = GenericRow.of(
                42,                                         // INT
                100L,                                       // BIGINT
                true,                                       // BOOLEAN
                3.14d,                                      // DOUBLE
                BinaryString.fromString("hello")            // VARCHAR
        );

        assertThat(PaimonAuditLogSystemTable.formatField(row, 0, new IntType()))
                .isEqualTo("42");
        assertThat(PaimonAuditLogSystemTable.formatField(row, 1, new BigIntType()))
                .isEqualTo("100");
        assertThat(PaimonAuditLogSystemTable.formatField(row, 2, new BooleanType()))
                .isEqualTo("true");
        assertThat(PaimonAuditLogSystemTable.formatField(row, 3, new DoubleType()))
                .isEqualTo("3.14");
        assertThat(PaimonAuditLogSystemTable.formatField(row, 4, VarCharType.STRING_TYPE))
                .isEqualTo("hello");
    }

    private static Catalog buildCatalog() {
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    private static void writeMultiTypeData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();

        // Row 1: all fields populated
        GenericRow row1 = GenericRow.of(
                42,                                                    // id (INT)
                100000L,                                               // amount (BIGINT)
                Decimal.fromBigDecimal(new BigDecimal("99.99"), 10, 2), // price (DECIMAL(10,2))
                true,                                                  // active (BOOLEAN)
                3.14d,                                                 // score (DOUBLE)
                1.5f,                                                  // ratio (FLOAT)
                BinaryString.fromString("test_item"),                  // name (STRING)
                Timestamp.fromLocalDateTime(                           // created_at (TIMESTAMP(3))
                        LocalDateTime.of(2024, 1, 15, 10, 30, 0))
        );
        write.write(row1);

        // Row 2: some null fields to test null handling
        GenericRow row2 = GenericRow.of(
                99,                                                    // id (INT)
                200000L,                                               // amount (BIGINT)
                Decimal.fromBigDecimal(new BigDecimal("0.01"), 10, 2), // price (DECIMAL(10,2))
                false,                                                 // active (BOOLEAN)
                null,                                                  // score (DOUBLE) - NULL
                2.0f,                                                  // ratio (FLOAT)
                null,                                                  // name (STRING) - NULL
                Timestamp.fromLocalDateTime(                           // created_at (TIMESTAMP(3))
                        LocalDateTime.of(2024, 6, 1, 0, 0, 0))
        );
        write.write(row2);

        List<CommitMessage> messages = write.prepareCommit();
        write.close();

        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        commit.close();
    }
}
