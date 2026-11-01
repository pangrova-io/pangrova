package org.pangrova.testing.benchmark.jmh;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.pangrova.common.json.jackson.JsonMapperFactory;
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
 * JMH benchmarks for JSON serialization and deserialization.
 *
 * <p>Measures the throughput of serializing and deserializing typical query metadata
 * payloads using the shared Jackson ObjectMapper configuration. This is a critical
 * hot path in REST API responses, query plan serialization, and metadata exchange.</p>
 *
 * <p><b>Note:</b> This benchmark covers the JSON (Jackson) serialization path used
 * in REST APIs and metadata exchange. The wire-protocol (protobuf) serialization
 * path is benchmarked separately in native execution tests.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgs = {"--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class JsonSerializationBenchmark {

    private ObjectWriter writer;
    private ObjectReader reader;
    private QueryMetadataPayload smallPayload;
    private QueryMetadataPayload largePayload;
    private byte[] smallPayloadBytes;
    private byte[] largePayloadBytes;

    @Setup
    public void setup() throws JsonProcessingException {
        writer = JsonMapperFactory.writer();
        reader = JsonMapperFactory.reader().forType(QueryMetadataPayload.class);

        smallPayload = new QueryMetadataPayload(
                "query-001",
                "SELECT * FROM orders WHERE id = 1",
                Instant.parse("2024-06-15T10:30:00Z"),
                42L,
                "COMPLETED",
                List.of("orders"),
                Map.of("rows_scanned", 1000L, "bytes_read", 65536L)
        );

        largePayload = new QueryMetadataPayload(
                "query-002",
                "SELECT o.id, o.customer_id, o.total, c.name, c.email "
                        + "FROM orders o JOIN customers c ON o.customer_id = c.id "
                        + "WHERE o.created_at >= '2024-01-01' AND o.status = 'SHIPPED' "
                        + "GROUP BY o.id, o.customer_id, o.total, c.name, c.email "
                        + "HAVING o.total > 100 ORDER BY o.total DESC LIMIT 1000",
                Instant.parse("2024-06-15T10:30:00Z"),
                3847L,
                "COMPLETED",
                List.of("orders", "customers", "line_items", "products", "categories"),
                Map.of(
                        "rows_scanned", 5_000_000L,
                        "bytes_read", 2_147_483_648L,
                        "rows_returned", 1000L,
                        "shuffle_bytes", 104_857_600L,
                        "spill_bytes", 0L,
                        "peak_memory", 536_870_912L
                )
        );

        smallPayloadBytes = writer.writeValueAsBytes(smallPayload);
        largePayloadBytes = writer.writeValueAsBytes(largePayload);
    }

    @Benchmark
    public void serializeSmallPayload(Blackhole bh) throws JsonProcessingException {
        bh.consume(writer.writeValueAsBytes(smallPayload));
    }

    @Benchmark
    public void serializeLargePayload(Blackhole bh) throws JsonProcessingException {
        bh.consume(writer.writeValueAsBytes(largePayload));
    }

    @Benchmark
    public void deserializeSmallPayload(Blackhole bh) throws Exception {
        bh.consume(reader.readValue(smallPayloadBytes));
    }

    @Benchmark
    public void deserializeLargePayload(Blackhole bh) throws Exception {
        bh.consume(reader.readValue(largePayloadBytes));
    }

    /**
     * Representative query metadata payload for serialization benchmarks.
     */
    public record QueryMetadataPayload(
            String queryId,
            String sql,
            Instant submittedAt,
            long durationMs,
            String status,
            List<String> tablesAccessed,
            Map<String, Long> metrics
    ) {
        public QueryMetadataPayload {
            tablesAccessed = List.copyOf(tablesAccessed);
            metrics = Map.copyOf(metrics);
        }

        @Override public List<String> tablesAccessed() { return List.copyOf(tablesAccessed); }
        @Override public Map<String, Long> metrics() { return Map.copyOf(metrics); }
    }
}
