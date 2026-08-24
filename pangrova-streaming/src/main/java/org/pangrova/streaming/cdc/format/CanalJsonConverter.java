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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Converts Canal JSON CDC messages into {@link CdcEnvelope}.
 *
 * <p>Expected payload structure:
 * <pre>{@code
 * {
 *   "data": [ { "id": 1, "name": "Alice" } ],
 *   "old":  [ { "id": 1, "name": "Bob" } ],   // present for UPDATE/DELETE
 *   "type": "INSERT" | "UPDATE" | "DELETE",
 *   "database": "mydb",
 *   "table": "users",
 *   "ts": 1234567890000
 * }
 * }</pre>
 *
 * <p>Canal emits one JSON message per DML statement, with {@code data} containing
 * all affected rows. Each row produces a separate {@link CdcEnvelope}. For single-row
 * {@link #deserialize(byte[])}, only the first row is returned.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class CanalJsonConverter implements CdcRecordConverter {

    private static final String FORMAT_ID = "canal-json";

    private final ObjectMapper objectMapper = JsonMapperFactory.create();

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public CdcEnvelope deserialize(byte[] payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            return buildFirstEnvelope(root);
        } catch (CdcRecordConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new CdcRecordConversionException(
                    "Failed to parse Canal JSON message", e);
        }
    }

    /**
     * Canal messages may contain multiple rows per message. This override
     * expands each row into a separate envelope, routing parse failures
     * to the dead-letter callback.
     */
    @Override
    public List<CdcEnvelope> convertBatch(List<byte[]> payloads,
                                          BiConsumer<byte[], Exception> deadLetter) {
        List<CdcEnvelope> results = new ArrayList<>();
        for (byte[] payload : payloads) {
            try {
                JsonNode root = objectMapper.readTree(payload);
                results.addAll(buildAllEnvelopes(root));
            } catch (Exception e) {
                deadLetter.accept(payload, e);
            }
        }
        return results;
    }

    private CdcEnvelope buildFirstEnvelope(JsonNode root) {
        List<CdcEnvelope> envelopes = buildAllEnvelopes(root);
        if (envelopes.isEmpty()) {
            throw new CdcRecordConversionException(
                    "Canal JSON 'data' array is empty", null);
        }
        return envelopes.get(0);
    }

    private List<CdcEnvelope> buildAllEnvelopes(JsonNode root) {
        CdcOperation op = requireOp(root);
        JsonNode dataArray = requireDataArray(root);
        JsonNode oldArray = root.get("old");
        String tableName = resolveTableName(root);
        Instant eventTime = resolveEventTime(root);
        Map<String, String> sourceInfo = Map.of(
                "database", textOrEmpty(root, "database"),
                "table", textOrEmpty(root, "table"));
        return buildEnvelopeList(dataArray, oldArray, op, tableName, eventTime, sourceInfo);
    }

    private static CdcOperation requireOp(JsonNode root) {
        JsonNode typeNode = root.get("type");
        if (typeNode == null || typeNode.isNull()) {
            throw new CdcRecordConversionException(
                    "Canal JSON missing required 'type' field", null);
        }
        return mapOp(typeNode.asText());
    }

    private static JsonNode requireDataArray(JsonNode root) {
        JsonNode dataArray = root.get("data");
        if (dataArray == null || !dataArray.isArray()) {
            throw new CdcRecordConversionException(
                    "Canal JSON missing or invalid 'data' array", null);
        }
        return dataArray;
    }

    private static String resolveTableName(JsonNode root) {
        String database = textOrEmpty(root, "database");
        String table = textOrEmpty(root, "table");
        return database.isEmpty() ? table : database + "." + table;
    }

    private static Instant resolveEventTime(JsonNode root) {
        JsonNode tsNode = root.get("ts");
        return (tsNode != null && !tsNode.isNull())
                ? Instant.ofEpochMilli(tsNode.asLong())
                : Instant.EPOCH;
    }

    private static List<CdcEnvelope> buildEnvelopeList(JsonNode dataArray, JsonNode oldArray,
                                                       CdcOperation op, String tableName,
                                                       Instant eventTime, Map<String, String> sourceInfo) {
        List<CdcEnvelope> envelopes = new ArrayList<>(dataArray.size());
        for (int i = 0; i < dataArray.size(); i++) {
            CdcRow after = (op == CdcOperation.DELETE)
                    ? null : CdcRowParser.toCdcRow(dataArray.get(i));
            CdcRow before = resolveBefore(dataArray, oldArray, op, i);
            envelopes.add(new CdcEnvelope(op, tableName, before, after,
                    eventTime, 0, sourceInfo));
        }
        return envelopes;
    }

    private static CdcRow resolveBefore(JsonNode dataArray, JsonNode oldArray,
                                        CdcOperation op, int index) {
        if (op == CdcOperation.DELETE) {
            return CdcRowParser.toCdcRow(dataArray.get(index));
        }
        if (oldArray != null && oldArray.isArray() && index < oldArray.size()) {
            return CdcRowParser.toCdcRow(oldArray.get(index));
        }
        return null;
    }

    private static CdcOperation mapOp(String type) {
        return switch (type.toUpperCase()) {
            case "INSERT" -> CdcOperation.INSERT;
            case "UPDATE" -> CdcOperation.UPDATE;
            case "DELETE" -> CdcOperation.DELETE;
            default -> throw new CdcRecordConversionException(
                    "Unknown Canal operation type: " + type, null);
        };
    }

    private static String textOrEmpty(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return (node != null && !node.isNull()) ? node.asText() : "";
    }
}
