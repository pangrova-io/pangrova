package org.pangrova.coordinator.dynamictable.detect;

import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.pangrova.spi.streaming.CdcLifecycleManager.CdcRegistrationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Detects source-table changes via CDC stream consumption.
 *
 * <p>Polls a batch of CDC events from the last committed checkpoint offset,
 * validates schema versions, and returns a {@link ChangeResult.CdcBatch}.
 * On connection interruption, retries from the same checkpoint offset
 * (R4 AC6). Integrates with {@link CdcLifecycleManager} for stream
 * registration and heartbeat renewal (R4 AC7).</p>
 *
 * <p>Constructor dependencies: {@link CdcEventSource}, {@link CdcLifecycleManager},
 * {@code maxRetries}, {@code pollTimeout} — total 4, compliant with the
 * max-4-constructor-deps rule.</p>
 */
public final class CdcChangeDetector implements ChangeDetector {

    private static final Logger LOG = LoggerFactory.getLogger(CdcChangeDetector.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final int DEFAULT_MAX_CONCURRENT_WRITES = 1;
    private static final long DEFAULT_MAX_PENDING_BYTES = 64 * 1024 * 1024;

    private final CdcEventSource eventSource;
    private final CdcLifecycleManager cdcLifecycle;
    private final int maxRetries;
    private final Duration pollTimeout;

    public CdcChangeDetector(CdcEventSource eventSource,
                             CdcLifecycleManager cdcLifecycle,
                             int maxRetries,
                             Duration pollTimeout) {
        this.eventSource = Objects.requireNonNull(eventSource, "eventSource must not be null");
        this.cdcLifecycle = Objects.requireNonNull(cdcLifecycle, "cdcLifecycle must not be null");
        this.maxRetries = maxRetries;
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
    }

    public CdcChangeDetector(CdcEventSource eventSource,
                             CdcLifecycleManager cdcLifecycle) {
        this(eventSource, cdcLifecycle, DEFAULT_MAX_RETRIES, DEFAULT_POLL_TIMEOUT);
    }

    @Override
    public ChangeResult detect(DynamicTableState state) {
        Objects.requireNonNull(state, "state must not be null");

        CdcRegistrationResult registration = registerStream(state);
        try {
            List<CdcEnvelope> events = pollWithRetry(state, registration);
            if (events.isEmpty()) {
                return ChangeResult.empty();
            }
            validateSchemaVersions(state, events);
            return ChangeResult.cdcBatch(events);
        } finally {
            renewHeartbeat(registration.streamId());
        }
    }

    // ── Stream registration (R4 AC7) ───────────────────────────────

    private CdcRegistrationResult registerStream(DynamicTableState state) {
        String consumerId = "dt-cdc-" + state.dtName();
        CdcRegistrationResult result = cdcLifecycle.register(
                consumerId, state.tenantId(),
                DEFAULT_MAX_CONCURRENT_WRITES, DEFAULT_MAX_PENDING_BYTES);
        LOG.info("event=cdc_stream_registered dtName={} streamId={} heartbeatIntervalMs={}",
                state.dtName(), result.streamId(), result.heartbeatIntervalMs());
        return result;
    }

    private void renewHeartbeat(String streamId) {
        try {
            cdcLifecycle.heartbeat(streamId);
        } catch (Exception e) {
            LOG.warn("event=cdc_heartbeat_failed streamId={} error={}",
                    streamId, e.getMessage());
        }
    }

    // ── Poll with retry on connection interruption (R4 AC6) ────────

    private List<CdcEnvelope> pollWithRetry(DynamicTableState state,
                                            CdcRegistrationResult registration) {
        String offset = state.checkpoint();
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                List<CdcEnvelope> events = eventSource.poll(offset, pollTimeout);
                if (attempt > 0) {
                    LOG.info("event=cdc_connection_recovered dtName={} attempt={}",
                            state.dtName(), attempt);
                }
                return events;
            } catch (Exception e) {
                handlePollFailure(state, registration, attempt, e);
            }
        }
        throw new IllegalStateException(
                "CDC poll exhausted retries for dtName=" + state.dtName());
    }

    private void handlePollFailure(DynamicTableState state,
                                   CdcRegistrationResult registration,
                                   int attempt, Exception cause) {
        if (attempt >= maxRetries) {
            LOG.error("event=cdc_poll_exhausted dtName={} maxRetries={} error={}",
                    state.dtName(), maxRetries, cause.getMessage());
            throw new RuntimeException(
                    "CDC connection failed after " + maxRetries
                            + " retries for dtName=" + state.dtName(), cause);
        }
        LOG.warn("event=cdc_connection_interrupted dtName={} attempt={} error={} action=retry_from_checkpoint",
                state.dtName(), attempt, cause.getMessage());
        renewHeartbeat(registration.streamId());
    }

    // ── Schema version validation (R14 AC4) ────────────────────────

    private void validateSchemaVersions(DynamicTableState state,
                                        List<CdcEnvelope> events) {
        int expected = state.sourceSchemaVersion();
        for (CdcEnvelope event : events) {
            if (event.schemaVersion() != expected) {
                LOG.error("event=schema_version_mismatch dtName={} expected={} actual={}",
                        state.dtName(), expected, event.schemaVersion());
                throw new SchemaEvolutionException(
                        expected, event.schemaVersion(), state.dtName());
            }
        }
    }
}
