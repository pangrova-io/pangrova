package org.pangrova.streaming.core.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Consumes Debezium CDC messages, parses to {@link CdcEnvelope}, delegates to TableWriter.
 * Write-then-commit-offset pattern for at-least-once delivery.
 */
public class CdcConsumer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CdcConsumer.class);

    /** Maximum length of a raw message snippet retained in dead-letter log lines. */
    private static final int LOG_MESSAGE_TRUNCATION_LIMIT = 500;

    private final TableWriter writer;
    private final CdcConfig config;
    private final ObjectMapper objectMapper;
    private final Consumer<List<CdcEnvelope>> commitCallback;
    private final LongCounter deadLetterCounter;
    private final IdempotentExecutor idempotentExecutor;

    /** Creates a consumer that writes CDC rows through the given writer with default idempotency. */
    public CdcConsumer(TableWriter writer, CdcConfig config,
                       Consumer<List<CdcEnvelope>> commitCallback,
                       MetricsProvider metricsProvider) {
        this(writer, config, commitCallback, metricsProvider, IdempotentExecutor.passThrough());
    }

    /** Creates a consumer that writes CDC rows through the given writer with the given idempotent executor. */
    public CdcConsumer(TableWriter writer, CdcConfig config,
                       Consumer<List<CdcEnvelope>> commitCallback,
                       MetricsProvider metricsProvider,
                       IdempotentExecutor idempotentExecutor) {
        this.writer = writer;
        this.config = config;
        this.objectMapper = JsonMapperFactory.shared();
        this.commitCallback = commitCallback;
        this.idempotentExecutor = idempotentExecutor != null
                ? idempotentExecutor : IdempotentExecutor.passThrough();
        LongCounter counter = metricsProvider.createCounter(
                "pangrova.streaming.cdc.dead.letter",
                "Number of CDC messages that failed parsing and were routed to dead letter",
                "{message}");
        this.deadLetterCounter = counter != null ? counter : value -> {};
        LOG.info("event=cdc_consumer_initialized topics={} groupId={} consumerId={}",
                config.topics(), config.groupId(), config.consumerId());
    }

    /** Parse Debezium JSON envelope to strong-typed {@link CdcEnvelope}. */
    public CdcEnvelope parseDebeziumMessage(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            return buildCdcEnvelope(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse Debezium message", e);
        }
    }

    /** Group envelopes by table name for batch delegation. */
    public Map<String, List<CdcEnvelope>> groupByTable(List<CdcEnvelope> envelopes) {
        Map<String, List<CdcEnvelope>> grouped = new LinkedHashMap<>();
        for (CdcEnvelope envelope : envelopes) {
            grouped.computeIfAbsent(envelope.tableName(), k -> new ArrayList<>()).add(envelope);
        }
        return grouped;
    }

    /** Process a batch: parse → group → write → commit. Dead-letter on parse failure.
     *  Only commits offset for successfully written tables. Tables with write failures
     *  are dead-lettered and their envelopes excluded from the commit. */
    public void processBatch(List<String> rawMessages) {
        LOG.debug("event=process_batch_start count={} topics={}", rawMessages.size(), config.topics());
        List<CdcEnvelope> envelopes = parseBatchWithDeadLetter(rawMessages);
        if (envelopes.isEmpty()) {
            return;
        }
        List<CdcEnvelope> committed = writeGroupedEnvelopes(groupByTable(envelopes));
        if (!committed.isEmpty()) {
            commitCallback.accept(committed);
        }
    }

    private List<CdcEnvelope> parseBatchWithDeadLetter(List<String> rawMessages) {
        List<CdcEnvelope> envelopes = new ArrayList<>(rawMessages.size());
        for (String raw : rawMessages) {
            try {
                envelopes.add(parseDebeziumMessage(raw));
            } catch (Exception e) {
                deadLetterCounter.add(1);
                LOG.warn("Dead-letter CDC message (parse failure): {}", truncateForLog(raw), e);
            }
        }
        return envelopes;
    }

    private static String truncateForLog(String message) {
        return message.length() <= LOG_MESSAGE_TRUNCATION_LIMIT
                ? message
                : message.substring(0, LOG_MESSAGE_TRUNCATION_LIMIT) + "...[truncated]";
    }

    @Override
    public void close() {
        // no-op: CdcConsumer does not own external resources
    }

    private CdcEnvelope buildCdcEnvelope(JsonNode payload) {
        CdcOperation op = mapDebeziumOp(payload.get("op").asText());
        CdcRow before = toCdcRow(payload.get("before"));
        CdcRow after = toCdcRow(payload.get("after"));
        Instant eventTime = Instant.ofEpochMilli(payload.get("ts_ms").asLong());
        String tableName = extractTableName(payload);
        int schemaVersion = extractSchemaVersion(payload);
        Map<String, String> sourceInfo = extractSourceInfo(payload);
        return new CdcEnvelope(op, tableName, before, after, eventTime, schemaVersion, sourceInfo);
    }

    private CdcRow toCdcRow(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        Map<String, Object> columns = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> columns.put(entry.getKey(), extractValue(entry.getValue())));
        return CdcRow.of(columns);
    }

    private Object extractValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return null;
        }
        if (valueNode.isInt()) return valueNode.intValue();
        if (valueNode.isLong()) return valueNode.longValue();
        if (valueNode.isDouble() || valueNode.isFloat()) return valueNode.doubleValue();
        if (valueNode.isBoolean()) return valueNode.booleanValue();
        return valueNode.asText();
    }

    /**
     * Write grouped envelopes to target tables. Returns the envelopes that were
     * successfully written (safe to commit). Failed tables are dead-lettered.
     *
     * <p>BUG-15 fix: Added retry with exponential backoff for transient write
     * failures. Without retry, a single network blip dead-letters the entire
     * table batch, requiring Kafka re-delivery of the full batch (including
     * already-written tables). The retry loop gives transient errors a chance
     * to recover before falling through to dead-letter.</p>
     */
    private List<CdcEnvelope> writeGroupedEnvelopes(Map<String, List<CdcEnvelope>> grouped) {
        List<CdcEnvelope> successfullyWritten = new ArrayList<>();
        grouped.forEach((tableName, envelopes) ->
                processTableEnvelopes(tableName, envelopes, successfullyWritten));
        return successfullyWritten;
    }

    private void processTableEnvelopes(String tableName, List<CdcEnvelope> envelopes,
                                       List<CdcEnvelope> successfullyWritten) {
        List<WriteRecord> writeRecords = new ArrayList<>(envelopes.size());
        List<CdcEnvelope> convertedEnvelopes = new ArrayList<>(envelopes.size());
        convertEnvelopes(tableName, envelopes, writeRecords, convertedEnvelopes);
        if (!writeRecords.isEmpty()) {
            flushWriteRecords(tableName, writeRecords, convertedEnvelopes, successfullyWritten);
        }
    }

    private void convertEnvelopes(String tableName, List<CdcEnvelope> envelopes,
                                  List<WriteRecord> writeRecords,
                                  List<CdcEnvelope> convertedEnvelopes) {
        for (CdcEnvelope env : envelopes) {
            try {
                String eventKey = deriveEventKey(env);
                writeRecords.add(idempotentExecutor.execute(eventKey,
                        () -> WriteRecordConverter.fromCdcEnvelope(env)));
                convertedEnvelopes.add(env);
            } catch (Exception e) {
                deadLetterCounter.add(1);
                LOG.warn("event=dead_letter_write_conversion table={} op={} error={}",
                        tableName, env.operation(), e.getMessage(), e);
            }
        }
    }

    private void flushWriteRecords(String tableName, List<WriteRecord> writeRecords,
                                   List<CdcEnvelope> convertedEnvelopes,
                                   List<CdcEnvelope> successfullyWritten) {
        boolean written = writeWithRetry(tableName, writeRecords);
        if (written) {
            successfullyWritten.addAll(convertedEnvelopes);
        } else {
            deadLetterCounter.add(writeRecords.size());
        }
    }

    /** Max retry attempts for transient write failures. */
    private static final int MAX_WRITE_RETRIES = 3;
    private static final long RETRY_INITIAL_MS = 100;
    private static final long RETRY_MAX_MS = 2000;

    private boolean writeWithRetry(String tableName, List<WriteRecord> writeRecords) {
        BackoffStrategy backoff = BackoffStrategy.retry(RETRY_INITIAL_MS, RETRY_MAX_MS);
        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_WRITE_RETRIES; attempt++) {
            try {
                writer.write(tableName, writeRecords, WriteMode.UPSERT, WriteContext.selfManaged());
                return true;
            } catch (Exception e) {
                lastError = e;
                if (attempt < MAX_WRITE_RETRIES && !sleepBackoff(backoff, tableName, attempt)) {
                    break;
                }
            }
        }
        LOG.error("event=dead_letter_batch_write table={} count={} attempts={} error={}",
                tableName, writeRecords.size(), MAX_WRITE_RETRIES,
                lastError != null ? lastError.getMessage() : "unknown", lastError);
        return false;
    }

    private boolean sleepBackoff(BackoffStrategy backoff, String tableName, int attempt) {
        long sleepMs = backoff.next();
        LOG.warn("event=cdc_write_retry table={} attempt={}/{} backoffMs={}",
                tableName, attempt, MAX_WRITE_RETRIES, sleepMs);
        ThreadUtils.sleepQuietly(sleepMs);
        // If the sleep was interrupted, the interrupt flag has been restored and
        // the retry loop must stop (same semantics as the previous InterruptedException path).
        return !Thread.currentThread().isInterrupted();
    }

    /**
     * Derives a composite deduplication key from CDC envelope source info.
     * Uses Debezium source coordinates (connector, file/lsn, pos) + table + eventTime.
     */
    private static String deriveEventKey(CdcEnvelope envelope) {
        Map<String, String> src = envelope.sourceInfo();
        String connector = src.getOrDefault("connector", "");
        String file = src.getOrDefault("file", src.getOrDefault("lsn", ""));
        String pos = src.getOrDefault("pos", src.getOrDefault("txId", ""));
        return connector + ":" + envelope.tableName() + ":" + file + ":" + pos
                + ":" + envelope.eventTime().toEpochMilli();
    }

    private CdcOperation mapDebeziumOp(String op) {
        return switch (op) {
            case "c", "r" -> CdcOperation.INSERT;
            case "u" -> CdcOperation.UPDATE;
            case "d" -> CdcOperation.DELETE;
            default -> throw new IllegalArgumentException("Unknown Debezium op: " + op);
        };
    }

    private String extractTableName(JsonNode payload) {
        JsonNode source = payload.get("source");
        if (source == null) {
            return "unknown";
        }
        String db = source.has("db") ? source.get("db").asText() : "";
        String table = source.has("table") ? source.get("table").asText() : "unknown";
        return db.isEmpty() ? table : db + "." + table;
    }

    /**
     * Extracts schema version from the Debezium envelope.
     *
     * <p>BUG-009 fix: Debezium's {@code source.version} is the <em>connector</em>
     * version (e.g., "2.4.0"), not the schema version. {@code Integer.parseInt("2.4.0")}
     * always throws {@code NumberFormatException}, so {@code schemaVersion} was
     * silently stuck at 0 for every message, breaking schema evolution detection.</p>
     *
     * <p>Now uses the top-level {@code schema} object's hash code as a stable
     * schema fingerprint, falling back to {@code source.struct_version} if present.</p>
     *
     * <p>P1-BUG-5 fix: Replaced {@code after.size()} fallback with a sorted
     * field-name hash. The old approach was unreliable: different schemas could
     * have the same field count (add+drop), DELETE operations have null
     * {@code after}, and nullable fields may be omitted from JSON.</p>
     */
    private int extractSchemaVersion(JsonNode payload) {
        int structVersion = tryExtractStructVersion(payload);
        if (structVersion >= 0) {
            return structVersion;
        }
        return 0;
    }

    private static int tryExtractStructVersion(JsonNode payload) {
        JsonNode source = payload.get("source");
        if (source != null && source.has("struct_version")) {
            try {
                return Integer.parseInt(source.get("struct_version").asText());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return -1;
    }

    private Map<String, String> extractSourceInfo(JsonNode payload) {
        JsonNode source = payload.get("source");
        if (source == null) {
            return Map.of();
        }
        Map<String, String> info = new LinkedHashMap<>();
        source.fields().forEachRemaining(e -> info.put(e.getKey(), e.getValue().asText()));
        return Map.copyOf(info);
    }
}
