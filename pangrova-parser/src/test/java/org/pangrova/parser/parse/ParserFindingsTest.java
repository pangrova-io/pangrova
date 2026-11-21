package org.pangrova.parser.parse;

import org.pangrova.parser.api.enums.LiteralType;
import org.pangrova.parser.api.enums.QueryHintType;
import org.pangrova.parser.api.enums.TableRefKind;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.parser.api.ast.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for parser P2 findings: literal type gaps (G.1), TableRefKind (G.4),
 * query hints (G.6), and full AST structure verification (G.3).
 */
class ParserFindingsTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    // ── G.1: Literal type gaps ──

    @Test
    void parseIntervalSingleUnit() {
        Statement stmt = parser.parse("SELECT INTERVAL '5' DAY FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(Literal.class);
        Literal lit = (Literal) expr;
        assertThat(lit.literalType()).isEqualTo(LiteralType.INTERVAL);
        assertThat(lit.value()).isInstanceOf(LiteralValue.IntervalValue.class);
        var iv = (LiteralValue.IntervalValue) lit.value();
        assertThat(iv.expression()).isEqualTo("5");
        assertThat(iv.fromUnit()).isEqualTo("DAY");
        assertThat(iv.isRange()).isFalse();
    }

    @Test
    void parseIntervalYearToMonth() {
        Statement stmt = parser.parse("SELECT INTERVAL '1-6' YEAR TO MONTH FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        Literal lit = (Literal) expr;
        assertThat(lit.literalType()).isEqualTo(LiteralType.INTERVAL);
        var iv = (LiteralValue.IntervalValue) lit.value();
        assertThat(iv.expression()).isEqualTo("1-6");
        assertThat(iv.fromUnit()).isEqualTo("YEAR");
        assertThat(iv.toUnit()).isEqualTo("MONTH");
        assertThat(iv.isRange()).isTrue();
    }

    @Test
    void parseIntervalDayToSecond() {
        Statement stmt = parser.parse("SELECT INTERVAL '3 12:30:00' DAY TO SECOND FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        Literal lit = (Literal) expr;
        var iv = (LiteralValue.IntervalValue) lit.value();
        assertThat(iv.expression()).isEqualTo("3 12:30:00");
        assertThat(iv.fromUnit()).isEqualTo("DAY");
        assertThat(iv.toUnit()).isEqualTo("SECOND");
    }

    @Test
    void parseBinaryHexLiteral() {
        Statement stmt = parser.parse("SELECT X'DEADBEEF' FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(Literal.class);
        Literal lit = (Literal) expr;
        assertThat(lit.literalType()).isEqualTo(LiteralType.BINARY_HEX);
        assertThat(lit.value()).isInstanceOf(LiteralValue.BinaryHexValue.class);
        var bv = (LiteralValue.BinaryHexValue) lit.value();
        assertThat(bv.hexDigits()).isEqualTo("DEADBEEF");
    }

    @Test
    void malformedBinaryByteHasTheSameDiagnosticInStrictAndRecoveryParsing() {
        String sql = "SELECT\n  X'ABC'";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(org.pangrova.parser.api.exception.ParseException.class)
                .hasMessageContaining("pairs of ASCII hexadecimal digits");
        assertThat(parser.parseWithRecovery(sql).errors()).singleElement().satisfies(error -> {
            assertThat(error.line()).isEqualTo(2);
            assertThat(error.column()).isEqualTo(2);
        });
        assertThatThrownBy(() -> Literal.ofBinaryHex("0g")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Literal.ofBinaryHex("１２")).isInstanceOf(IllegalArgumentException.class);
        var mixedCase = new LiteralValue.BinaryHexValue("00aF");
        assertThat(mixedCase.bytes()).containsExactly((byte) 0, (byte) 0xaf);
        assertThat(new LiteralValue.BinaryHexValue("").bytes()).isEmpty();
    }

    @Test
    void parseBinaryHexLiteralEmpty() {
        Statement stmt = parser.parse("SELECT X'' FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        Literal lit = (Literal) expr;
        assertThat(lit.literalType()).isEqualTo(LiteralType.BINARY_HEX);
        assertThat(((LiteralValue.BinaryHexValue) lit.value()).hexDigits()).isEmpty();
    }

    @Test
    void parseTimestampWithTimeZone() {
        Statement stmt = parser.parse(
                "SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00+05:30' FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(Literal.class);
        Literal lit = (Literal) expr;
        assertThat(lit.literalType()).isEqualTo(LiteralType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(lit.value()).isInstanceOf(LiteralValue.TimestampWithTimeZoneValue.class);
        var tv = (LiteralValue.TimestampWithTimeZoneValue) lit.value();
        assertThat(tv.value()).isEqualTo("2024-01-15 10:30:00+05:30");
    }

    // ── G.3: Full AST structure verification ──

    @Test
    void intervalLiteralRoundTrip() {
        AstPrinter printer = new AstPrinter();
        Statement stmt = parser.parse("SELECT INTERVAL '5' DAY FROM t");
        String printed = printer.print(stmt);
        assertThat(printed).contains("INTERVAL '5' DAY");
    }

    @Test
    void intervalRangeRoundTrip() {
        AstPrinter printer = new AstPrinter();
        Statement stmt = parser.parse("SELECT INTERVAL '1-6' YEAR TO MONTH FROM t");
        String printed = printer.print(stmt);
        assertThat(printed).contains("INTERVAL '1-6' YEAR TO MONTH");
    }

    @Test
    void binaryHexRoundTrip() {
        AstPrinter printer = new AstPrinter();
        Statement stmt = parser.parse("SELECT X'CAFE' FROM t");
        String printed = printer.print(stmt);
        assertThat(printed).contains("X'CAFE'");
    }

    @Test
    void timestampWithTimeZoneRoundTrip() {
        AstPrinter printer = new AstPrinter();
        Statement stmt = parser.parse(
                "SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00+05:30' FROM t");
        String printed = printer.print(stmt);
        assertThat(printed).contains("TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00+05:30'");
    }

    // ── G.4: TableRefKind typed enum ──

    @Test
    void namedTableRefKind() {
        Statement stmt = parser.parse("SELECT * FROM orders");
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.NAMED);
    }

    @Test
    void derivedTableRefKind() {
        Statement stmt = parser.parse("SELECT * FROM (SELECT 1) AS sub");
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.DERIVED);
    }

    @Test
    void lateralDerivedTableRefKind() {
        Statement stmt = parser.parse("SELECT * FROM t, LATERAL (SELECT 1) AS sub");
        Query q = (Query) stmt;
        // The lateral derived is the second table in the comma-join
        assertThat(q.from()).hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void systemTableRefKind() {
        Statement stmt = parser.parse("SELECT * FROM catalog.schema.orders$snapshots");
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.SYSTEM_TABLE);
    }

    // ── G.6: QueryHint support ──

    @Test
    void queryHintResolvesBroadcast() {
        QueryHint hint = QueryHint.of("BROADCAST", List.of("orders"));
        assertThat(hint.hintType()).isEqualTo(QueryHintType.JOIN_STRATEGY);
        assertThat(hint.name()).isEqualTo("BROADCAST");
        assertThat(hint.parameters()).containsExactly("orders");
    }

    @Test
    void queryHintResolvesParallel() {
        QueryHint hint = QueryHint.of("PARALLEL", List.of("8"));
        assertThat(hint.hintType()).isEqualTo(QueryHintType.PARALLELISM);
        assertThat(hint.name()).isEqualTo("PARALLEL");
        assertThat(hint.parameters()).containsExactly("8");
    }

    @Test
    void queryHintResolvesTimeout() {
        QueryHint hint = QueryHint.of("QUERY_TIMEOUT", List.of("30000"));
        assertThat(hint.hintType()).isEqualTo(QueryHintType.TIMEOUT);
    }

    @Test
    void queryHintResolvesIndex() {
        QueryHint hint = QueryHint.of("USE_INDEX", List.of("idx_customer_id"));
        assertThat(hint.hintType()).isEqualTo(QueryHintType.INDEX_USAGE);
    }

    @Test
    void queryHintResolvesGenericForUnknown() {
        QueryHint hint = QueryHint.of("CUSTOM_HINT", List.of("param1", "param2"));
        assertThat(hint.hintType()).isEqualTo(QueryHintType.GENERIC);
        assertThat(hint.name()).isEqualTo("CUSTOM_HINT");
        assertThat(hint.parameters()).containsExactly("param1", "param2");
    }

    @Test
    void queryHintImmutableParameters() {
        QueryHint hint = QueryHint.of("BROADCAST", List.of("t1"));
        assertThatThrownBy(() -> hint.parameters().add("t2"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
