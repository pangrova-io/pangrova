package org.pangrova.streaming.provider.cdcformat.maxwell;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConversionException;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.core.cdc.CdcRowParser;

import java.time.Instant;
import java.util.Map;

/**
 * Converts Maxwell JSON CDC messages into {@link CdcEnvelope}.
 *
 * <p>Expected payload structure:
 * <pre>{@code
 * {
 *   "database": "mydb",
 *   "table": "users",
 *   "type": "insert" | "update" | "delete",
 *   "data": { "id": 1, "name": "Alice" },
 *   "old":  { "name": "Bob" },              // present for update only
 *   "ts": 1234567890
 * }
 * }</pre>
 *
 * <p>Maxwell emits one JSON message per row change. The {@code ts} field is
 * a Unix epoch in seconds (not milliseconds like Debezium).</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class MaxwellJsonConverter implements CdcRecordConverter {

    private static final String FORMAT_ID = "maxwell-json";

    private final ObjectMapper objectMapper = JsonMapperFactory.create();

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public CdcEnvelope deserialize(byte[] payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            return buildEnvelope(root);
        } catch (CdcRecordConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new CdcRecordConversionException(
                    "Failed to parse Maxwell JSON message", e);
        }
    }

    private CdcEnvelope buildEnvelope(JsonNode root) {
        JsonNode typeNode = root.get("type");
        if (typeNode == null || typeNode.isNull()) {
            throw new CdcRecordConversionException(
                    "Maxwell JSON missing required 'type' field", null);
        }
        CdcOperation op = mapOp(typeNode.asText());

        String database = textOrEmpty(root, "database");
        String table = textOrEmpty(root, "table");
        String tableName = database.isEmpty() ? table : database + "." + table;

        CdcRow after = (op == CdcOperation.DELETE)
                ? null : CdcRowParser.toCdcRow(root.get("data"));
        CdcRow before = (op == CdcOperation.DELETE)
                ? CdcRowParser.toCdcRow(root.get("data"))
                : CdcRowParser.toCdcRow(root.get("old"));

        Instant eventTime = extractEventTime(root);
        Map<String, String> sourceInfo = Map.of("database", database, "table", table);
        return new CdcEnvelope(op, tableName, before, after, eventTime, 0, sourceInfo);
    }

    private static Instant extractEventTime(JsonNode root) {
        JsonNode tsNode = root.get("ts");
        return (tsNode != null && !tsNode.isNull())
                ? Instant.ofEpochSecond(tsNode.asLong())
                : Instant.EPOCH;
    }

    private CdcOperation mapOp(String type) {
        return switch (type.toLowerCase()) {
            case "insert" -> CdcOperation.INSERT;
            case "update" -> CdcOperation.UPDATE;
            case "delete" -> CdcOperation.DELETE;
            default -> throw new CdcRecordConversionException(
                    "Unknown Maxwell operation type: " + type, null);
        };
    }

    private static String textOrEmpty(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return (node != null && !node.isNull()) ? node.asText() : "";
    }
}
