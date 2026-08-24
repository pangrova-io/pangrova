package org.pangrova.parser.property;

import net.jqwik.api.*;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property: ALTER TABLE ADD COLUMN ... DEFAULT round-trip correctness.
 *
 * <p><b>Validates: Requirements A2</b></p>
 *
 * <p>For any valid ALTER TABLE ADD COLUMN with DEFAULT literal,
 * {@code parse(print(ast))} must produce a semantically equivalent AST.</p>
 */
class DefaultColumnValueRoundTripPropertyTest {

    private final SqlParser parser = new SqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Property(tries = 10)
    void alterTableAddColumnDefaultRoundTrip(
            @ForAll("addColumnDefaultSql") String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2)
                .as("Round-trip for [%s] → [%s]", sql, printed)
                .isEqualTo(ast1);
    }

    @Provide
    Arbitrary<String> addColumnDefaultSql() {
        return Arbitraries.oneOf(
                integerDefaults(),
                stringDefaults(),
                decimalDefaults(),
                booleanDefaults(),
                noDefault()
        );
    }

    private Arbitrary<String> integerDefaults() {
        return Arbitraries.of(
                "ALTER TABLE t ADD COLUMN col BIGINT DEFAULT 42",
                "ALTER TABLE t ADD COLUMN col INTEGER DEFAULT 0",
                "ALTER TABLE t ADD COLUMN col BIGINT DEFAULT -1",
                "ALTER TABLE my_schema.my_table ADD COLUMN new_col BIGINT DEFAULT 100"
        );
    }

    private Arbitrary<String> stringDefaults() {
        return Arbitraries.of(
                "ALTER TABLE t ADD COLUMN col VARCHAR DEFAULT 'hello'",
                "ALTER TABLE t ADD COLUMN status VARCHAR DEFAULT 'active'",
                "ALTER TABLE t ADD COLUMN tag VARCHAR DEFAULT ''"
        );
    }

    private Arbitrary<String> decimalDefaults() {
        return Arbitraries.of(
                "ALTER TABLE t ADD COLUMN col DOUBLE DEFAULT 3.14",
                "ALTER TABLE t ADD COLUMN score DOUBLE DEFAULT 0.0"
        );
    }

    private Arbitrary<String> booleanDefaults() {
        return Arbitraries.of(
                "ALTER TABLE t ADD COLUMN col BOOLEAN DEFAULT TRUE",
                "ALTER TABLE t ADD COLUMN active BOOLEAN DEFAULT FALSE"
        );
    }

    private Arbitrary<String> noDefault() {
        return Arbitraries.of(
                "ALTER TABLE t ADD COLUMN col BIGINT",
                "ALTER TABLE t ADD COLUMN name VARCHAR"
        );
    }
}
