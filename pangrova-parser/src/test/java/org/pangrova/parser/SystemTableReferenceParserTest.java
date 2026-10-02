package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Verifies that {@code table$systemTable} syntax is parsed into
 * {@link SystemTableReference} and round-trips through {@link AstPrinter}.
 */
class SystemTableReferenceParserTest {

    private final SqlParser parser = new SqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Test
    void simpleSystemTableReference() {
        Statement stmt = parser.parse("SELECT * FROM orders$snapshots");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;
        assertThat(query.from()).hasSize(1);

        TableRef ref = query.from().get(0);
        assertThat(ref.isSystemTable()).isTrue();

        SystemTableReference sysRef = ref.systemTableRef().orElseThrow();
        assertThat(sysRef.baseTable().parts()).containsExactly("orders");
        assertThat(sysRef.systemTableName()).isEqualTo("snapshots");
    }

    @Test
    void qualifiedSystemTableReference() {
        Statement stmt = parser.parse("SELECT * FROM catalog.schema.orders$files");
        Query query = (Query) stmt;
        TableRef ref = query.from().get(0);
        assertThat(ref.isSystemTable()).isTrue();

        SystemTableReference sysRef = ref.systemTableRef().orElseThrow();
        assertThat(sysRef.baseTable().parts()).containsExactly("catalog", "schema", "orders");
        assertThat(sysRef.systemTableName()).isEqualTo("files");
    }

    @Test
    void systemTableWithAlias() {
        Statement stmt = parser.parse("SELECT * FROM orders$snapshots AS s");
        Query query = (Query) stmt;
        TableRef ref = query.from().get(0);
        assertThat(ref.isSystemTable()).isTrue();
        assertThat(ref.alias()).hasValue("s");

        SystemTableReference sysRef = ref.systemTableRef().orElseThrow();
        assertThat(sysRef.baseTable().parts()).containsExactly("orders");
        assertThat(sysRef.systemTableName()).isEqualTo("snapshots");
    }

    @Test
    void systemTableBaseTableNamePreservedInTableRef() {
        Statement stmt = parser.parse("SELECT * FROM mydb.public.events$manifests");
        Query query = (Query) stmt;
        TableRef ref = query.from().get(0);
        assertThat(ref.isSystemTable()).isTrue();

        // TableRef.name() should equal the base table (without system table suffix)
        assertThat(ref.name().parts()).containsExactly("mydb", "public", "events");
    }

    @Test
    void regularTableWithoutDollarIsNotSystemTable() {
        Statement stmt = parser.parse("SELECT * FROM orders");
        Query query = (Query) stmt;
        TableRef ref = query.from().get(0);
        assertThat(ref.isSystemTable()).isFalse();
        assertThat(ref.systemTableRef()).isEmpty();
    }

    @Test
    void quotedDollarTableNameFailsClosedInsteadOfResolvingSystemTable() {
        assertThatThrownBy(() -> parser.parse("SELECT * FROM \"orders$snapshots\""))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Quoted table names containing '$' are not supported");
    }

    @Test
    void roundTripSimpleSystemTable() {
        String sql = "SELECT * FROM orders$snapshots";
        Statement stmt = parser.parse(sql);
        String printed = printer.print(stmt);
        assertThat(printed).isEqualTo("SELECT * FROM orders$snapshots");
    }

    @Test
    void roundTripQualifiedSystemTable() {
        String sql = "SELECT * FROM catalog.schema.orders$files";
        Statement stmt = parser.parse(sql);
        String printed = printer.print(stmt);
        assertThat(printed).isEqualTo("SELECT * FROM catalog.schema.orders$files");
    }

    @Test
    void roundTripSystemTableWithAlias() {
        String sql = "SELECT * FROM orders$snapshots AS s";
        Statement stmt = parser.parse(sql);
        String printed = printer.print(stmt);
        assertThat(printed).isEqualTo("SELECT * FROM orders$snapshots AS s");
    }

    @Test
    void variousSystemTableNames() {
        for (String sysTable : new String[]{"snapshots", "files", "manifests", "history",
                "partitions", "refs", "entries", "schemas", "options", "tags", "branches"}) {
            String sql = "SELECT * FROM t$" + sysTable;
            Statement stmt = parser.parse(sql);
            Query query = (Query) stmt;
            TableRef ref = query.from().get(0);
            assertThat(ref.isSystemTable())
                    .as("Expected system table for t$%s", sysTable)
                    .isTrue();
            assertThat(ref.systemTableRef().orElseThrow().systemTableName()).isEqualTo(sysTable);

            // Round-trip
            String printed = printer.print(stmt);
            assertThat(printed).isEqualTo("SELECT * FROM t$" + sysTable);
        }
    }
}
