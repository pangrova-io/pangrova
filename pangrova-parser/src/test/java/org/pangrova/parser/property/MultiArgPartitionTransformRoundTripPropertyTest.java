package org.pangrova.parser.property;

import net.jqwik.api.*;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property: multi-argument partition transform round-trip correctness.
 *
 * <p><b>Validates: Requirements A5</b></p>
 *
 * <p>For any valid ALTER TABLE SET PARTITION SPEC with single-arg or multi-arg
 * transforms, {@code parse(print(ast))} must produce a semantically equivalent AST.</p>
 */
class MultiArgPartitionTransformRoundTripPropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Property(tries = 10)
    void singleArgTransformRoundTrip(
            @ForAll("singleArgTransformSql") String sql) {
        assertRoundTrip(sql);
    }

    @Property(tries = 10)
    void multiArgTransformRoundTrip(
            @ForAll("multiArgTransformSql") String sql) {
        assertRoundTrip(sql);
    }

    @Property(tries = 10)
    void mixedTransformRoundTrip(
            @ForAll("mixedTransformSql") String sql) {
        assertRoundTrip(sql);
    }

    @Property(tries = 10)
    void multiArgTransformPreservesAllArguments(
            @ForAll("multiArgTransformSql") String sql) {
        Statement ast = parser.parse(sql);
        assertThat(ast).isInstanceOf(AlterTableStatement.SetPartitionSpec.class);
        AlterTableStatement.SetPartitionSpec spec = (AlterTableStatement.SetPartitionSpec) ast;
        for (AlterTableStatement.PartitionTransform t : spec.transforms()) {
            if (!t.isIdentity()) {
                assertThat(t.arguments())
                        .as("Transform %s should have arguments", t.functionName())
                        .isNotEmpty();
            }
        }
    }

    @Property(tries = 10)
    void identityTransformRoundTrip(
            @ForAll("identityTransformSql") String sql) {
        assertRoundTrip(sql);
    }

    private void assertRoundTrip(String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2)
                .as("Round-trip for [%s] → [%s]", sql, printed)
                .isEqualTo(ast1);
    }

    @Provide
    Arbitrary<String> singleArgTransformSql() {
        return Arbitraries.of(
                "ALTER TABLE t SET PARTITION SPEC (year(ts))",
                "ALTER TABLE t SET PARTITION SPEC (month(created_at))",
                "ALTER TABLE t SET PARTITION SPEC (day(event_date))",
                "ALTER TABLE t SET PARTITION SPEC (hour(log_time))",
                "ALTER TABLE my_schema.my_table SET PARTITION SPEC (year(ts))"
        );
    }

    @Provide
    Arbitrary<String> multiArgTransformSql() {
        return Arbitraries.of(
                "ALTER TABLE t SET PARTITION SPEC (bucket(id, 16))",
                "ALTER TABLE t SET PARTITION SPEC (bucket(col1, col2, 16))",
                "ALTER TABLE t SET PARTITION SPEC (truncate(name, 10))",
                "ALTER TABLE t SET PARTITION SPEC (truncate(city, 5))",
                "ALTER TABLE t SET PARTITION SPEC (bucket(region, category, 32))"
        );
    }

    @Provide
    Arbitrary<String> mixedTransformSql() {
        return Arbitraries.of(
                "ALTER TABLE t SET PARTITION SPEC (category, month(ts))",
                "ALTER TABLE t SET PARTITION SPEC (region, bucket(id, 16))",
                "ALTER TABLE t SET PARTITION SPEC (year(ts), bucket(col1, col2, 16))",
                "ALTER TABLE t SET PARTITION SPEC (category, year(ts), bucket(id, 8))",
                "ALTER TABLE t SET PARTITION SPEC (region, truncate(name, 10), day(ts))"
        );
    }

    @Provide
    Arbitrary<String> identityTransformSql() {
        return Arbitraries.of(
                "ALTER TABLE t SET PARTITION SPEC (category)",
                "ALTER TABLE t SET PARTITION SPEC (region, category)",
                "ALTER TABLE my_schema.my_table SET PARTITION SPEC (region)"
        );
    }
}
