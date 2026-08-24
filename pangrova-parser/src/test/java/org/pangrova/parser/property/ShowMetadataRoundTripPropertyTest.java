package org.pangrova.parser.property;

import net.jqwik.api.*;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property: SHOW SNAPSHOTS/FILES/MANIFESTS/PARTITIONS round-trip correctness.
 *
 * <p><b>Validates: Requirements B3</b></p>
 *
 * <p>For any valid SHOW metadata statement with various table name forms,
 * {@code parse(print(ast))} must produce a semantically equivalent AST.</p>
 */
class ShowMetadataRoundTripPropertyTest {

    private final SqlParser parser = new SqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Property(tries = 10)
    void showMetadataRoundTrip(@ForAll("showMetadataSql") String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2)
                .as("Round-trip for [%s] → [%s]", sql, printed)
                .isEqualTo(ast1);
    }

    @Provide
    Arbitrary<String> showMetadataSql() {
        return Arbitraries.oneOf(
                showSnapshots(),
                showFiles(),
                showManifests(),
                showPartitions()
        );
    }

    private Arbitrary<String> showSnapshots() {
        return Arbitraries.of(
                "SHOW SNAPSHOTS FROM my_table",
                "SHOW SNAPSHOTS FROM catalog.schema.my_table",
                "SHOW SNAPSHOTS FROM schema.orders"
        );
    }

    private Arbitrary<String> showFiles() {
        return Arbitraries.of(
                "SHOW FILES FROM my_table",
                "SHOW FILES FROM catalog.schema.my_table",
                "SHOW FILES FROM schema.events"
        );
    }

    private Arbitrary<String> showManifests() {
        return Arbitraries.of(
                "SHOW MANIFESTS FROM my_table",
                "SHOW MANIFESTS FROM catalog.schema.my_table",
                "SHOW MANIFESTS FROM schema.lineage"
        );
    }

    private Arbitrary<String> showPartitions() {
        return Arbitraries.of(
                "SHOW PARTITIONS FROM my_table",
                "SHOW PARTITIONS FROM catalog.schema.my_table",
                "SHOW PARTITIONS FROM schema.metrics"
        );
    }
}
