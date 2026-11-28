package org.pangrova.parser.property.ast.statement;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.syntax.AntlrSqlParser;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property test for missing required clauses in CREATE STREAM SQL.
 *
 * <p>The grammar requires FROM KAFKA (...), FORMAT '...', and INTO &lt;table&gt;
 * clauses. Any CREATE STREAM statement missing one or more of these required
 * clauses must produce a parse error.
 *
 * <p><b>Validates: Requirements 1.4</b></p>
 */
class StreamParseErrorPropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    /**
     * Property 3: CREATE STREAM without FROM KAFKA clause produces a parse error.
     */
    @Property(tries = 10)
    void missingFromKafkaClauseProducesParseError(
            @ForAll("streamNames") String streamName) {

        String sql = "CREATE STREAM " + streamName
                + " FORMAT 'debezium-json'"
                + " INTO target_table";

        assertThatThrownBy(() -> parser.parse(sql))
                .as("CREATE STREAM without FROM KAFKA should fail: %s", sql)
                .isInstanceOf(ParseException.class);
    }

    /**
     * Property 3: CREATE STREAM without INTO clause produces a parse error.
     */
    @Property(tries = 10)
    void missingIntoClauseProducesParseError(
            @ForAll("streamNames") String streamName) {

        String sql = "CREATE STREAM " + streamName
                + " FROM KAFKA ('bootstrap.servers' = 'broker:9092', 'topic' = 'events')"
                + " FORMAT 'debezium-json'";

        assertThatThrownBy(() -> parser.parse(sql))
                .as("CREATE STREAM without INTO should fail: %s", sql)
                .isInstanceOf(ParseException.class);
    }

    /**
     * Property 3: CREATE STREAM without FORMAT clause produces a parse error.
     */
    @Property(tries = 10)
    void missingFormatClauseProducesParseError(
            @ForAll("streamNames") String streamName) {

        String sql = "CREATE STREAM " + streamName
                + " FROM KAFKA ('bootstrap.servers' = 'broker:9092', 'topic' = 'events')"
                + " INTO target_table";

        assertThatThrownBy(() -> parser.parse(sql))
                .as("CREATE STREAM without FORMAT should fail: %s", sql)
                .isInstanceOf(ParseException.class);
    }

    /**
     * Property 3: CREATE STREAM with only the stream name (missing everything)
     * produces a parse error.
     */
    @Property(tries = 10)
    void onlyStreamNameProducesParseError(
            @ForAll("streamNames") String streamName) {

        String sql = "CREATE STREAM " + streamName;

        assertThatThrownBy(() -> parser.parse(sql))
                .as("CREATE STREAM with only name should fail: %s", sql)
                .isInstanceOf(ParseException.class);
    }

    // ── Generators ──

    @Provide
    Arbitrary<String> streamNames() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withChars('_')
                .ofMinLength(2).ofMaxLength(15)
                .filter(s -> Character.isLetter(s.charAt(0)))
                .filter(s -> !isReservedWord(s));
    }

    private static boolean isReservedWord(String s) {
        return RESERVED_WORDS.contains(s.toUpperCase());
    }

    private static final java.util.Set<String> RESERVED_WORDS = java.util.Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
            "ORDER", "AS", "ON", "AND", "OR", "NOT", "TRUE", "FALSE",
            "ASC", "DESC", "CREATE", "DROP", "ALTER", "FOR", "OF",
            "ALL", "ANY", "SOME", "DISTINCT", "UNION", "NULL", "IN"
    );
}
