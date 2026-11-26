/* Persistent benchmark transport adapter; not part of the engine execution plane. */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.client.exception.PangrovaConnectionException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.sql.Types;
import java.util.Map;

/** One JSON request per immutable SQL step; a single REST client owns the stream. */
public final class TpcClient {
    private static final ObjectMapper JSON = new ObjectMapper();

    public record Config(String host, int port, String catalog, String schema, boolean tls,
                         boolean allowPlaintextResults, int requestTimeout, String tokenEnv) {}
    public record Request(String sql, int timeout) {}

    public static void main(String[] arguments) throws Exception {
        // Dependencies may log to System.out. Reserve the original descriptor for JSONL only.
        var protocol = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
        System.setOut(System.err);
        Config config = JSON.readValue(Path.of(arguments[0]).toFile(), Config.class);
        if (config.requestTimeout() < 1) throw new IllegalArgumentException("request timeout required");
        var builder = PangrovaRestClient.builder()
                .coordinatorHost(config.host()).coordinatorPort(config.port()).useTls(config.tls())
                .requestTimeout(Duration.ofSeconds(config.requestTimeout())).retryPolicy(RetryPolicy.NONE)
                .resultTransportSecurity(ResultTransportSecurity.systemTrust(!config.allowPlaintextResults()));
        if (config.tokenEnv() != null) {
            String token = System.getenv(config.tokenEnv());
            if (token == null || token.isBlank()) throw new IllegalArgumentException("token environment is blank");
            builder.bearerToken(token);
        }
        try (var client = builder.build();
             var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            ObjectNode ready = JSON.createObjectNode().put("ready", true).put("bridgeVersion", 1)
                    .put("javaVersion", System.getProperty("java.runtime.version"));
            protocol.println(JSON.writeValueAsString(ready));
            for (String line; (line = input.readLine()) != null;) {
                Request request = JSON.readValue(line, Request.class);
                protocol.println(JSON.writeValueAsString(execute(client, config, request, protocol)));
                if (protocol.checkError()) throw new IllegalStateException("protocol output failed");
            }
        }
    }

    private static ObjectNode execute(PangrovaRestClient client, Config config,
                                      Request request, PrintWriter protocol) {
        QueryExecution query = null;
        long started = System.nanoTime();
        try {
            if (request.sql() == null || request.sql().isBlank() || request.timeout() < 1) {
                throw new IllegalArgumentException("SQL and positive query timeout required");
            }
            long deadline = Math.addExact(started, Duration.ofSeconds(request.timeout()).toNanos());
            long epochDeadline = Math.addExact(System.currentTimeMillis(), request.timeout() * 1000L);
            query = client.beginQuery(request.sql(), Map.of("catalog", config.catalog(), "schema", config.schema()));
            var submitted = query.submission();
            String queryId = submitted.queryId();
            protocol.println(JSON.writeValueAsString(JSON.createObjectNode()
                    .put("event", "submitted").put("queryId", queryId)));
            ObjectNode result = null;
            if (submitted.resultDisposition() == ResultDisposition.STREAM && readable(submitted.state())) {
                try (var cursor = client.openResult(submitted.resultEndpoint(), epochDeadline)) {
                    result = result(cursor);
                }
            }
            while (System.nanoTime() < deadline) {
                var target = result == null ? QueryWaitTarget.RESULT_OR_TERMINAL : QueryWaitTarget.TERMINAL;
                QueryStatusResponse status = query.awaitStatus(target, epochDeadline);
                String state = status.state();
                if ("FAILED".equals(state) || "CANCELLED".equals(state)) {
                    return failure(query, "server-" + state.toLowerCase());
                }
                if (result == null && status.resultDisposition() == ResultDisposition.STREAM && readable(state)) {
                    try (var cursor = client.openResult(status.resultEndpoint(), epochDeadline)) {
                        result = result(cursor);
                    }
                }
                if ("FINISHED".equals(state)) {
                    if (System.nanoTime() >= deadline) {
                        return failure(query, "query-timeout");
                    }
                    if (result == null && status.resultDisposition() == ResultDisposition.NONE) {
                        double elapsedMs = (System.nanoTime() - started) / 1_000_000.0;
                        return JSON.createObjectNode().put("queryId", queryId)
                                .put("state", "FINISHED").put("resultConsumed", false).put("elapsedMs", elapsedMs);
                    }
                    if (result == null) {
                        return failure(query, "missing-result-endpoint");
                    }
                    double elapsedMs = (System.nanoTime() - started) / 1_000_000.0;
                    ObjectNode response = JSON.createObjectNode().put("queryId", queryId)
                            .put("state", "FINISHED").put("resultConsumed", true).put("elapsedMs", elapsedMs);
                    response.set("result", result);
                    response.set("serverStats", JSON.valueToTree(status.stats()));
                    return response;
                }
                if (Thread.currentThread().isInterrupted()) return failure(query, "interrupted");
            }
            return failure(query, "query-timeout");
        } catch (Exception error) {
            // Never publish arbitrary client/server exception messages or secret-bearing diagnostics.
            String reason = error instanceof PangrovaConnectionException connection
                    ? switch (connection.errorCode()) {
                        case "QUERY_STATUS_DEADLINE_EXCEEDED" -> "query-timeout";
                        case "CONNECTION_INTERRUPTED" -> "interrupted";
                        default -> error.getClass().getSimpleName();
                    } : error.getClass().getSimpleName();
            ObjectNode response = failure(query, reason);
            if (error.getStackTrace().length > 0) response.put("errorLocation", error.getStackTrace()[0].toString());
            return response;
        }
    }

    private static boolean readable(String state) {
        return "RUNNING".equals(state) || "FINISHING".equals(state) || "FINISHED".equals(state);
    }

    private static ObjectNode failure(QueryExecution query, String error) {
        String cancellation = "not-submitted";
        boolean interrupted = Thread.interrupted();
        try {
            if (query != null) cancellation = query.cancel().name();
        } catch (Exception cleanup) {
            cancellation = "request-failed";
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
        return JSON.createObjectNode().put("state", "FAILED")
                .put("queryId", query == null ? null : query.submission().queryId())
                .put("error", error).put("cancellation", cancellation).put("resourceDrain", "unverified");
    }

    private static ObjectNode result(ResultCursor cursor) {
        ObjectNode result = JSON.createObjectNode();
        ArrayNode columns = result.putArray("columns");
        ArrayNode nativeTypes = result.putArray("nativeTypes");
        for (var column : cursor.columns()) {
            columns.add(JSON.createObjectNode().put("name", column.name().toLowerCase(java.util.Locale.ROOT))
                    .put("type", family(column.sqlType())));
            nativeTypes.add(column.typeName());
        }
        ArrayNode rows = result.putArray("rows");
        while (cursor.next()) {
            ArrayNode row = rows.addArray();
            for (int index = 0; index < cursor.columns().size(); index++) {
                var column = cursor.columns().get(index);
                row.add(cell(cursor.value(index), family(column.sqlType()), column.typeName()));
            }
        }
        return result;
    }

    private static String family(int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> "integer";
            case Types.DOUBLE, Types.REAL, Types.FLOAT -> "float";
            case Types.DECIMAL, Types.NUMERIC -> "decimal";
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> "string";
            case Types.BOOLEAN, Types.BIT -> "boolean";
            case Types.DATE -> "date";
            default -> throw new IllegalArgumentException("unsupported result type");
        };
    }

    private static JsonNode cell(Object value, String family, String nativeType) {
        if (value == null) return JSON.nullNode();
        return switch (family) {
            case "decimal" -> JSON.createObjectNode().put("decimal", ((BigDecimal) value).toPlainString());
            case "date" -> {
                String date;
                if (value instanceof Number number) {
                    long days;
                    if ("Date(DAY)".equals(nativeType)) days = number.longValue();
                    else if ("Date(MILLISECOND)".equals(nativeType)) days = Math.floorDiv(number.longValue(), 86_400_000L);
                    else throw new IllegalArgumentException("unknown DATE storage unit");
                    date = LocalDate.ofEpochDay(days).toString();
                } else {
                    date = LocalDate.parse(value.toString()).toString();
                }
                yield JSON.createObjectNode().put("date", date);
            }
            case "string" -> JSON.getNodeFactory().textNode(value.toString());
            case "integer" -> JSON.getNodeFactory().numberNode(((Number) value).longValue());
            case "boolean" -> JSON.getNodeFactory().booleanNode((Boolean) value);
            case "float" -> {
                double number = ((Number) value).doubleValue();
                if (!Double.isFinite(number)) throw new IllegalArgumentException("non-finite result");
                yield JSON.getNodeFactory().numberNode(number);
            }
            default -> throw new IllegalArgumentException("unsupported cell");
        };
    }
}
