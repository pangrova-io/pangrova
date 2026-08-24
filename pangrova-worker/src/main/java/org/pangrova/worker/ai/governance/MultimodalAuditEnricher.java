/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.ai.governance;

import org.pangrova.spi.ai.inference.Modality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enriches audit trail with multimodal-specific events: FILE column accesses,
 * AI model invocations, and blob data reads.
 *
 * <p>Captures who accessed which FILE (blob URI), which model was invoked,
 * the input modality, estimated token cost, and the operation timestamp.
 * Events are buffered and flushed asynchronously to avoid impacting query latency.</p>
 *
 * <p>This component integrates with the existing {@code GovernanceAuditTrail} interface
 * via the {@link MultimodalAuditEvent} record which can be serialized to the
 * audit trail or security audit log.</p>
 *
 * <p>Thread-safe: uses a lock-free queue for event collection.</p>
 */
public final class MultimodalAuditEnricher {

    private static final Logger LOG = LoggerFactory.getLogger(MultimodalAuditEnricher.class);

    /** Maximum events buffered before forced flush. */
    private static final int MAX_BUFFER_SIZE = 10_000;

    private final ConcurrentLinkedQueue<MultimodalAuditEvent> eventBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger bufferSize = new AtomicInteger(0);
    private final AtomicLong totalEventsRecorded = new AtomicLong(0);
    private final AtomicLong totalEventsDropped = new AtomicLong(0);

    /**
     * Records a FILE access event (blob read/stream).
     *
     * @param principal   the user identity who accessed the file
     * @param queryId     the query that triggered the access
     * @param tableName   the qualified table name
     * @param columnName  the FILE column name
     * @param blobUri     the blob URI accessed
     * @param mimeType    the MIME type of the blob
     * @param sizeBytes   the size of the blob in bytes
     */
    public void recordFileAccess(String principal, String queryId, String tableName,
                                  String columnName, String blobUri, String mimeType,
                                  long sizeBytes) {
        MultimodalAuditEvent event = new MultimodalAuditEvent(
                EventType.FILE_ACCESS,
                Instant.now(),
                principal,
                queryId,
                tableName,
                columnName,
                blobUri,
                mimeType,
                null, // modelName
                null, // modality
                0L,   // estimatedTokens
                sizeBytes,
                Map.of());
        bufferEvent(event);
    }

    /**
     * Records a multimodal AI inference event.
     *
     * @param details identity, table and model attributes of the inference
     */
    public void recordMultimodalInference(MultimodalInferenceDetails details) {
        Modality modality = details.modality();
        MultimodalAuditEvent event = new MultimodalAuditEvent(
                EventType.MULTIMODAL_INFERENCE,
                Instant.now(),
                details.principal(),
                details.queryId(),
                details.tableName(),
                details.columnName(),
                details.blobUri(),
                details.mimeType(),
                details.modelName(),
                modality,
                details.estimatedTokens(),
                0L, // sizeBytes (already captured in FILE_ACCESS)
                Map.of("function_type", modality != null ? modality.name() : "UNKNOWN"));
        bufferEvent(event);
    }

    /**
     * Records a multimodal embedding event.
     *
     * @param principal       the user identity
     * @param queryId         the query that triggered embedding
     * @param tableName       the qualified table name
     * @param columnName      the FILE column embedded
     * @param blobUri         the blob URI
     * @param modelName       the embedding model used
     * @param estimatedTokens estimated token cost
     */
    public void recordMultimodalEmbedding(String principal, String queryId, String tableName,
                                           String columnName, String blobUri,
                                           String modelName, long estimatedTokens) {
        MultimodalAuditEvent event = new MultimodalAuditEvent(
                EventType.MULTIMODAL_EMBEDDING,
                Instant.now(),
                principal,
                queryId,
                tableName,
                columnName,
                blobUri,
                null,
                modelName,
                null,
                estimatedTokens,
                0L,
                Map.of("operation", "embedding"));
        bufferEvent(event);
    }

    /**
     * Drains all buffered events. Called by the async flush mechanism.
     *
     * @return array of events that were buffered since last drain
     */
    public MultimodalAuditEvent[] drainEvents() {
        int size = bufferSize.get();
        if (size == 0) {
            return new MultimodalAuditEvent[0];
        }
        MultimodalAuditEvent[] events = new MultimodalAuditEvent[Math.min(size, MAX_BUFFER_SIZE)];
        int i = 0;
        while (i < events.length) {
            MultimodalAuditEvent event = eventBuffer.poll();
            if (event == null) {
                break;
            }
            events[i++] = event;
            bufferSize.decrementAndGet();
        }
        if (i < events.length) {
            MultimodalAuditEvent[] trimmed = new MultimodalAuditEvent[i];
            System.arraycopy(events, 0, trimmed, 0, i);
            return trimmed;
        }
        return events;
    }

    /** Returns total events recorded since startup. */
    public long getTotalEventsRecorded() {
        return totalEventsRecorded.get();
    }

    /** Returns total events dropped due to buffer overflow. */
    public long getTotalEventsDropped() {
        return totalEventsDropped.get();
    }

    /** Returns current buffer size. */
    public int getBufferSize() {
        return bufferSize.get();
    }

    private void bufferEvent(MultimodalAuditEvent event) {
        if (bufferSize.get() >= MAX_BUFFER_SIZE) {
            totalEventsDropped.incrementAndGet();
            LOG.warn("event=multimodal_audit_buffer_full dropped_event_type={} principal={}",
                    event.eventType(), event.principal());
            return;
        }
        eventBuffer.offer(event);
        bufferSize.incrementAndGet();
        totalEventsRecorded.incrementAndGet();
    }

    /**
     * Types of multimodal audit events.
     */
    public enum EventType {
        /** A FILE blob was accessed (read/streamed). */
        FILE_ACCESS,
        /** A multimodal AI model was invoked with FILE input. */
        MULTIMODAL_INFERENCE,
        /** A FILE was embedded into a vector. */
        MULTIMODAL_EMBEDDING
    }

    /**
     * Immutable record capturing a multimodal audit event.
     *
     * @param eventType       category of the event
     * @param timestamp       when the event occurred
     * @param principal       user identity
     * @param queryId         the triggering query ID
     * @param tableName       qualified table name
     * @param columnName      the FILE column involved
     * @param blobUri         the blob URI accessed
     * @param mimeType        MIME type of the blob (nullable)
     * @param modelName       AI model used (nullable, for inference/embedding)
     * @param modality        input modality (nullable)
     * @param estimatedTokens estimated token cost (0 if not applicable)
     * @param sizeBytes       blob size in bytes (0 if not applicable)
     * @param metadata        additional event metadata
     */
    public record MultimodalAuditEvent(
            EventType eventType,
            Instant timestamp,
            String principal,
            String queryId,
            String tableName,
            String columnName,
            String blobUri,
            String mimeType,
            String modelName,
            Modality modality,
            long estimatedTokens,
            long sizeBytes,
            Map<String, String> metadata) {
    }
}
