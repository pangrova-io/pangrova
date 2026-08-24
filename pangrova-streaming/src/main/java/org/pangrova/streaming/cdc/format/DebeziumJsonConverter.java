package org.pangrova.streaming.cdc.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRecordConversionException;
import org.pangrova.spi.streaming.CdcRecordConverter;
import org.pangrova.spi.streaming.CdcRow;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts Debezium JSON CDC messages into {@link CdcEnvelope}.
 *
 * <p>Expected payload structure (with or without outer {@code payload} wrapper):
 * <pre>{@code
 * {
 *   "before": { ... },
 *   "after":  { ... },
 *   "source": { "db": "...", "table": "...", "version": "...", ... },
 *   "op": "c" | "u" | "d" | "r",
 *   "ts_ms": 1234567890
 * }
 * }</pre>
 *
 * <p>Delegates row parsing and schema version extraction to {@link CdcRowParser}.
 * Stateless and thread-safe.</p>
 */
public final class DebeziumJsonConverter implements CdcRecordConverter {

    private static final String FORMAT_ID = "debezium-json";

    private final ObjectMapper objectMapper = JsonMapperFactory.create();

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public CdcEnvelope deserialize(byte[] payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode body = root.has("payload") ? root.get("payload") : root;
            return buildEnvelope(body);
        } catch (CdcRecordConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new CdcRecordConversionException(
                    "Failed to parse Debezium JSON message", e);
        }
    }

    private CdcEnvelope buildEnvelope(JsonNode body) {
        JsonNode opNode = body.get("op");
        if (opNode == null || opNode.isNull()) {
            throw new CdcRecordConversionException(
                    "Debezium JSON missing required 'op' field", null);
        }
        CdcOperation op = mapOp(opNode.asText());
        CdcRow before = CdcRowParser.toCdcRow(body.get("before"));
        CdcRow after = CdcRowParser.toCdcRow(body.get("after"));

        JsonNode tsNode = body.get("ts_ms");
        Instant eventTime = (tsNode != null && !tsNode.isNull())
                ? Instant.ofEpochMilli(tsNode.asLong())
                : Instant.EPOCH;

        String tableName = extractTableName(body);
        int schemaVersion = CdcRowParser.extractSchemaVersion(body);
        Map<String, String> sourceInfo = extractSourceInfo(body);

        return new CdcEnvelope(op, tableName, before, after,
                eventTime, schemaVersion, sourceInfo);
    }

    private CdcOperation mapOp(String op) {
        return switch (op) {
            case "c", "r" -> CdcOperation.INSERT;
            case "u" -> CdcOperation.UPDATE;
            case "d" -> CdcOperation.DELETE;
            default -> throw new CdcRecordConversionException(
                    "Unknown Debezium op: " + op, null);
        };
    }

    private String extractTableName(JsonNode body) {
        JsonNode source = body.get("source");
        if (source == null) {
            return "unknown";
        }
        String db = source.has("db") ? source.get("db").asText() : "";
        String table = source.has("table") ? source.get("table").asText() : "unknown";
        return db.isEmpty() ? table : db + "." + table;
    }

    private Map<String, String> extractSourceInfo(JsonNode body) {
        JsonNode source = body.get("source");
        if (source == null) {
            return Map.of();
        }
        Map<String, String> info = new LinkedHashMap<>();
        source.fields().forEachRemaining(
                e -> info.put(e.getKey(), e.getValue().asText()));
        return Map.copyOf(info);
    }
}
