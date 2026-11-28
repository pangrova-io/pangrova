package org.pangrova.testing.benchmark.jmh;

import java.util.concurrent.TimeUnit;

import org.pangrova.parser.syntax.AntlrSqlParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for SQL parsing throughput.
 *
 * <p>Measures the throughput and latency of parsing SQL statements of varying
 * complexity, from simple literals to deeply nested arithmetic and function calls.
 * These represent the critical hot path in query compilation.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgs = {"--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class SqlParsingBenchmark {

    private AntlrSqlParser parser;

    private static final String SIMPLE_EXPR = "SELECT a + b FROM t";
    private static final String ARITHMETIC_EXPR =
            "SELECT (a * b + c) / (d - e) + f % g FROM t WHERE x > 10 AND y < 100";
    private static final String FUNCTION_EXPR =
            "SELECT COALESCE(UPPER(TRIM(name)), 'N/A'), ABS(price * quantity) FROM orders WHERE status = 'ACTIVE'";
    private static final String NESTED_EXPR =
            "SELECT CASE WHEN a > 0 THEN a * 2 WHEN a < 0 THEN ABS(a) ELSE 0 END, "
                    + "CAST(ts AS DATE), EXTRACT(YEAR FROM ts) FROM events "
                    + "WHERE category IN ('A', 'B', 'C') AND amount BETWEEN 100 AND 999";
    private static final String COMPLEX_EXPR =
            "SELECT o.id, SUM(l.quantity * l.price) AS total, "
                    + "COUNT(DISTINCT l.product_id) AS products, "
                    + "AVG(l.discount) AS avg_discount "
                    + "FROM orders o JOIN line_items l ON o.id = l.order_id "
                    + "WHERE o.created_at >= '2024-01-01' "
                    + "GROUP BY o.id HAVING SUM(l.quantity * l.price) > 1000 "
                    + "ORDER BY total DESC LIMIT 50";

    @Setup
    public void setup() {
        parser = new AntlrSqlParser();
    }

    @Benchmark
    public void parseSimpleExpression(Blackhole bh) {
        bh.consume(parser.parse(SIMPLE_EXPR));
    }

    @Benchmark
    public void parseArithmeticExpression(Blackhole bh) {
        bh.consume(parser.parse(ARITHMETIC_EXPR));
    }

    @Benchmark
    public void parseFunctionExpression(Blackhole bh) {
        bh.consume(parser.parse(FUNCTION_EXPR));
    }

    @Benchmark
    public void parseNestedExpression(Blackhole bh) {
        bh.consume(parser.parse(NESTED_EXPR));
    }

    @Benchmark
    public void parseComplexQuery(Blackhole bh) {
        bh.consume(parser.parse(COMPLEX_EXPR));
    }
}
