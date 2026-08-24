package org.pangrova.parser.property.ast.statement;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for CreateStream AST round-trip.
 *
 * <p>For any valid {@link CreateStreamStatement} AST node, printing the AST to SQL
 * and then parsing the resulting SQL string back produces an equivalent AST node.
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.5, 1.6</b></p>
 */
class CreateStreamRoundTripPropertyTest {

    private final SqlParser parser = new SqlParser();
    private final AstPrinter printer = new AstPrinter();

    /**
     * Property 1: CreateStream AST round-trip.
     *
     * <p>For any valid CreateStreamStatement, print→parse produces an equivalent AST node.
     */
    @Property(tries = 10)
    void createStreamRoundTrip(@ForAll("createStreamStatements") CreateStreamStatement original) {
        String sql = printer.print(original);
        Statement parsed = parser.parse(sql);

        assertThat(parsed)
                .as("Round-trip for SQL: %s", sql)
                .isInstanceOf(CreateStreamStatement.class);

        CreateStreamStatement result = (CreateStreamStatement) parsed;
        assertThat(result.name()).as("stream name").isEqualTo(original.name());
        assertThat(result.ifNotExists()).as("ifNotExists").isEqualTo(original.ifNotExists());
        assertThat(result.sourceProperties()).as("sourceProperties").isEqualTo(original.sourceProperties());
        assertThat(result.sourceFormat()).as("sourceFormat").isEqualTo(original.sourceFormat());
        assertThat(result.targetTable()).as("targetTable").isEqualTo(original.targetTable());
        assertThat(result.withProperties()).as("withProperties").isEqualTo(original.withProperties());
    }

    @Provide
    Arbitrary<CreateStreamStatement> createStreamStatements() {
        return Combinators.combine(
                identifiers(),
                Arbitraries.of(true, false),
                sourcePropertyMaps(),
                sourceFormats(),
                qualifiedIdentifiers(),
                withPropertyMaps()
        ).as(CreateStreamStatement::new);
    }

    // ── Generators ──

    /**
     * Generates simple stream names. Uses letter-starting identifiers that are safe
     * for SQL parsing (no reserved words that would conflict with grammar).
     */
    private Arbitrary<Identifier> identifiers() {
        return safeIdentifierStrings().map(name -> Identifier.of(name));
    }

    /**
     * Generates qualified identifiers (1-3 parts) for target table references
     * like catalog.schema.table.
     */
    private Arbitrary<Identifier> qualifiedIdentifiers() {
        return safeIdentifierStrings()
                .list().ofMinSize(1).ofMaxSize(3)
                .map(parts -> new Identifier(parts));
    }

    /**
     * Generates safe identifier strings: start with letter, contain only letters/digits/underscores,
     * length 1-20, avoiding SQL reserved words.
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
     * Generates source property maps with required keys (bootstrap.servers, topic)
     * plus optional extra properties.
     */
    private Arbitrary<Map<String, String>> sourcePropertyMaps() {
        Arbitrary<String> bootstrapServers = Arbitraries.of(
                "broker:9092", "broker1:9092,broker2:9092", "localhost:9092");
        Arbitrary<String> topics = safePropertyValues();
        Arbitrary<Map<String, String>> extraProps = stringPropertyMaps(0, 3);

        return Combinators.combine(bootstrapServers, topics, extraProps)
                .as((bs, topic, extra) -> {
                    var props = new LinkedHashMap<String, String>();
                    props.put("bootstrap.servers", bs);
                    props.put("topic", topic);
                    props.putAll(extra);
                    return Map.copyOf(props);
                });
    }

    /**
     * Generates CDC source format identifiers.
     */
    private Arbitrary<String> sourceFormats() {
        return Arbitraries.of("debezium-json", "debezium-avro", "canal-json", "maxwell-json");
    }

    /**
     * Generates optional WITH property maps (0-4 entries).
     */
    private Arbitrary<Map<String, String>> withPropertyMaps() {
        return stringPropertyMaps(0, 4);
    }

    /**
     * Generates string-to-string property maps with safe keys and values.
     */
    private Arbitrary<Map<String, String>> stringPropertyMaps(int minSize, int maxSize) {
        if (minSize == 0 && maxSize == 0) {
            return Arbitraries.just(Map.of());
        }
        return Arbitraries.maps(safePropertyKeys(), safePropertyValues())
                .ofMinSize(minSize).ofMaxSize(maxSize)
                .map(m -> Map.copyOf(m));
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
     * Includes words that would conflict with the CREATE STREAM grammar.
     */
    private static final java.util.Set<String> RESERVED_WORDS = java.util.Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
            "ORDER", "AS", "ON", "AND", "OR", "NOT", "TRUE", "FALSE",
            "ASC", "DESC", "CREATE", "DROP", "ALTER", "FOR", "OF",
            "ALL", "ANY", "SOME", "DISTINCT", "UNION", "NULL", "IN"
    );
}
