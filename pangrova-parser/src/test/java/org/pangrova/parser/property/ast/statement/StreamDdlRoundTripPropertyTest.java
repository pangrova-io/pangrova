package org.pangrova.parser.property.ast.statement;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.parser.syntax.AntlrSqlParser;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.DropStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.ShowStreamsStatement;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for AlterStream, DropStream, ShowStreams, and DescribeStream AST round-trip.
 *
 * <p>For any valid statement AST node, printing the AST to SQL and then parsing
 * the resulting SQL string back produces an equivalent AST node.
 *
 * <p><b>Validates: Requirements 2.1, 2.2, 2.3, 3.1, 3.2</b></p>
 */
class StreamDdlRoundTripPropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    // ── AlterStream round-trip ──

    @Example
    void preservesSourceProviderIndependentlyOfFormatAndProperties() {
        var parsed = (CreateStreamStatement) parser.parse("""
                CREATE STREAM changes FROM audit_feed ('channel'='orders')
                FORMAT 'debezium-json' INTO catalog.db.target
                """);
        assertThat(parsed.sourceProvider()).isEqualTo("audit_feed");
        assertThat(parser.parse(printer.print(parsed))).isEqualTo(parsed);
    }

    /**
     * Property 2a: AlterStream SUSPEND round-trip.
     *
     * <p><b>Validates: Requirements 2.1</b></p>
     */
    @Property(tries = 10)
    void alterStreamSuspendRoundTrip(@ForAll("alterStreamSuspendStatements") AlterStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(AlterStreamStatement.class);

        AlterStreamStatement result = (AlterStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
        assertThat(result.action()).as("action").isInstanceOf(AlterStreamAction.Suspend.class);
    }

    /**
     * Property 2b: AlterStream RESUME round-trip.
     *
     * <p><b>Validates: Requirements 2.2</b></p>
     */
    @Property(tries = 10)
    void alterStreamResumeRoundTrip(@ForAll("alterStreamResumeStatements") AlterStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(AlterStreamStatement.class);

        AlterStreamStatement result = (AlterStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
        assertThat(result.action()).as("action").isInstanceOf(AlterStreamAction.Resume.class);
    }

    /**
     * Property 2c: AlterStream SET properties round-trip.
     *
     * <p><b>Validates: Requirements 2.3</b></p>
     */
    @Property(tries = 10)
    void alterStreamSetPropertiesRoundTrip(@ForAll("alterStreamSetStatements") AlterStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(AlterStreamStatement.class);

        AlterStreamStatement result = (AlterStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
        assertThat(result.action()).as("action").isInstanceOf(AlterStreamAction.SetProperties.class);

        AlterStreamAction.SetProperties originalSet = (AlterStreamAction.SetProperties) original.action();
        AlterStreamAction.SetProperties resultSet = (AlterStreamAction.SetProperties) result.action();
        assertThat(resultSet.properties()).as("properties").isEqualTo(originalSet.properties());
    }

    // ── DropStream round-trip ──

    /**
     * Property 2d: DropStream round-trip.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b></p>
     */
    @Property(tries = 10)
    void dropStreamRoundTrip(@ForAll("dropStreamStatements") DropStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(DropStreamStatement.class);

        DropStreamStatement result = (DropStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
        assertThat(result.ifExists()).as("ifExists").isEqualTo(original.ifExists());
    }

    // ── ShowStreams round-trip ──

    /**
     * Property 2e: ShowStreams round-trip.
     *
     * <p><b>Validates: Requirements 2.1</b></p>
     */
    @Example
    void showStreamsRoundTrip() {
        ShowStreamsStatement original = new ShowStreamsStatement();
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(ShowStreamsStatement.class);
    }

    // ── DescribeStream round-trip ──

    /**
     * Property 2f: DescribeStream round-trip.
     *
     * <p><b>Validates: Requirements 2.1</b></p>
     */
    @Property(tries = 10)
    void describeStreamRoundTrip(@ForAll("describeStreamStatements") DescribeStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(DescribeStreamStatement.class);

        DescribeStreamStatement result = (DescribeStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
    }

    // ── Providers ──

    @Provide
    Arbitrary<AlterStreamStatement> alterStreamSuspendStatements() {
        return identifiers().map(name -> new AlterStreamStatement(name, new AlterStreamAction.Suspend()));
    }

    @Provide
    Arbitrary<AlterStreamStatement> alterStreamResumeStatements() {
        return identifiers().map(name -> new AlterStreamStatement(name, new AlterStreamAction.Resume()));
    }

    @Provide
    Arbitrary<AlterStreamStatement> alterStreamSetStatements() {
        return Combinators.combine(identifiers(), setPropertyMaps())
                .as((name, props) -> new AlterStreamStatement(name, new AlterStreamAction.SetProperties(props)));
    }

    @Provide
    Arbitrary<DropStreamStatement> dropStreamStatements() {
        return Combinators.combine(identifiers(), Arbitraries.of(true, false))
                .as(DropStreamStatement::new);
    }

    @Provide
    Arbitrary<DescribeStreamStatement> describeStreamStatements() {
        return identifiers().map(DescribeStreamStatement::new);
    }

    // ── Generators (reuse safe identifier patterns from CreateStreamRoundTripPropertyTest) ──

    /**
     * Generates simple stream names using letter-starting identifiers that are safe
     * for SQL parsing (no reserved words that would conflict with grammar).
     */
    private Arbitrary<Identifier> identifiers() {
        return safeIdentifierStrings().map(Identifier::of);
    }

    /**
     * Generates safe identifier strings: start with letter, contain only letters/digits/underscores,
     * length 2-15, avoiding SQL reserved words.
     */
    private Arbitrary<String> safeIdentifierStrings() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withChars('_')
                .ofMinLength(2).ofMaxLength(15)
                .filter(s -> Character.isLetter(s.charAt(0)))
                .filter(s -> !isReservedWord(s));
    }

    /**
     * Generates SET property maps with 1-4 entries (at least one property required for SET).
     */
    private Arbitrary<Map<String, String>> setPropertyMaps() {
        return Arbitraries.maps(safePropertyKeys(), safePropertyValues())
                .ofMinSize(1).ofMaxSize(4)
                .map(Map::copyOf);
    }

    /**
     * Generates safe property keys: lowercase with dots/underscores allowed.
     */
    private Arbitrary<String> safePropertyKeys() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withChars('_', '.')
                .ofMinLength(1).ofMaxLength(20)
                .filter(s -> Character.isLetter(s.charAt(0)))
                .filter(s -> !s.endsWith("."));
    }

    /**
     * Generates safe property values: alphanumeric with common punctuation,
     * no single quotes (which would break SQL string literals).
     */
    private Arbitrary<String> safePropertyValues() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('0', '9')
                .withChars('_', '-', '.', ':', ',', '/')
                .ofMinLength(1).ofMaxLength(30)
                .filter(s -> !s.isEmpty());
    }

    private static boolean isReservedWord(String s) {
        return RESERVED_WORDS.contains(s.toUpperCase());
    }

    /**
     * SQL reserved words that cannot be used as unquoted identifiers.
     * Includes words that would conflict with the Stream DDL grammar.
     */
    private static final java.util.Set<String> RESERVED_WORDS = java.util.Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
            "ORDER", "AS", "ON", "AND", "OR", "NOT", "TRUE", "FALSE",
            "ASC", "DESC", "CREATE", "DROP", "ALTER", "FOR", "OF",
            "ALL", "ANY", "SOME", "DISTINCT", "UNION", "NULL", "IN",
            "SET", "SUSPEND", "RESUME", "STREAM", "SHOW", "DESCRIBE"
    );
}
