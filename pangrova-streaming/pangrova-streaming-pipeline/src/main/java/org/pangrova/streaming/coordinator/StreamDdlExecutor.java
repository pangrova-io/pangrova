package org.pangrova.streaming.coordinator;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.StreamAlreadyExistsException;
import org.pangrova.common.exception.StreamNotFoundException;
import org.pangrova.common.exception.StreamQuotaExceededException;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.DropStreamStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Executes DDL operations for CDC ingestion streams:
 * CREATE, ALTER (SUSPEND/RESUME/SET_PROPERTIES), DROP, SHOW, DESCRIBE.
 *
 * <p>State machine:</p>
 * <pre>
 *   CREATE → RUNNING
 *   RUNNING  → SUSPENDED
 *   SUSPENDED → RUNNING (resume)
 *   FAILED   → RUNNING (resume)
 *   any      → DROPPING → DROPPED
 * </pre>
 *
 * <p>The durable {@link StreamStateStore} owns lifecycle state. The runtime discovers
 * runnable revisions independently.</p>
 */
@ThreadSafe
public final class StreamDdlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamDdlExecutor.class);

    private final StreamDdlContext deps;

    public StreamDdlExecutor(StreamDdlContext deps) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
    }

    // ── CREATE ──────────────────────────────────────────────────────

    /**
     * Creates a new ingestion stream.
     *
     * <p>Validates settings through the selected source factory,
     * checks per-tenant quota, and persists the complete running definition.</p>
     *
     * @param stmt            parsed CREATE STREAM statement
     * @param tenantId        owning tenant identifier
     * @param creatorIdentity identity of the user creating the stream
     */
    public void create(CreateStreamStatement stmt, String tenantId, String creatorIdentity) {
        String streamName = extractName(stmt.name());

        // Duplicate check
        Optional<StreamState> existing = stateStore().loadState(streamName);
        if (existing.isPresent()) {
            verifyTenant(existing.get(), tenantId, "create");
            if (stmt.ifNotExists()) {
                LOG.info("event=stream_create_skipped stream={} reason=already_exists", streamName);
                return;
            }
            throw new StreamAlreadyExistsException(
                    PangrovaErrorCode.STREAM_ALREADY_EXISTS,
                    "Stream already exists: " + streamName);
        }

        // Per-tenant quota check
        long activeCount = stateStore().listByTenant(tenantId).stream()
                .filter(s -> s.lifecycleState() == StreamLifecycleState.RUNNING
                        || s.lifecycleState() == StreamLifecycleState.CREATING)
                .count();
        if (activeCount >= deps.maxStreamsPerTenant()) {
            throw new StreamQuotaExceededException(
                    PangrovaErrorCode.STREAM_QUOTA_EXCEEDED,
                    "Tenant " + tenantId + " exceeds max streams limit: "
                            + deps.maxStreamsPerTenant());
        }

        // Validate source properties
        deps.sources().validate(stmt.sourceProvider(), stmt.sourceProperties());
        deps.converters().get(stmt.sourceFormat());

        Instant now = Instant.now();
        StreamState running = StreamState.builder()
                .streamName(streamName)
                .tenantId(tenantId)
                .lifecycleState(StreamLifecycleState.RUNNING)
                .targetTable(String.join(".", stmt.targetTable().parts()))
                .sourceProvider(stmt.sourceProvider())
                .sourceProperties(stmt.sourceProperties())
                .sourceFormat(stmt.sourceFormat())
                .streamProperties(stmt.withProperties() != null ? stmt.withProperties() : Map.of())
                .creatorIdentity(creatorIdentity)
                .coordinatorId("")
                .epoch(0)
                .schemaVersion(1)
                .createdAt(now)
                .updatedAt(now)
                .build();
        if (!stateStore().createState(running)) {
            // Another CREATE may have won after the initial lookup. Never upsert over it.
            loadOrThrow(streamName, tenantId);
            if (stmt.ifNotExists()) {
                return;
            }
            throw new StreamAlreadyExistsException(PangrovaErrorCode.STREAM_ALREADY_EXISTS,
                    "Stream already exists: " + streamName);
        }
        LOG.info("event=stream_created stream={} tenant={} target={} creator={}",
                streamName, tenantId, running.targetTable(), creatorIdentity);
    }

    // ── ALTER ───────────────────────────────────────────────────────

    /**
     * Alters an existing stream: SUSPEND, RESUME, or SET_PROPERTIES.
     *
     * <p>State machine validation:</p>
     * <ul>
     *   <li>SUSPEND: only from RUNNING</li>
     *   <li>RESUME: only from SUSPENDED or FAILED</li>
     *   <li>SET_PROPERTIES: any non-terminal state</li>
     * </ul>
     *
     * @param stmt     parsed ALTER STREAM statement
     * @param tenantId owning tenant identifier
     */
    public void alter(AlterStreamStatement stmt, String tenantId) {
        String streamName = extractName(stmt.name());
        StreamState state = loadOrThrow(streamName, tenantId);

        AlterStreamAction action = stmt.action();
        if (action instanceof AlterStreamAction.Suspend) {
            handleSuspend(streamName, state);
        } else if (action instanceof AlterStreamAction.Resume) {
            handleResume(streamName, state);
        } else if (action instanceof AlterStreamAction.SetProperties sp) {
            handleSetProperties(streamName, state, sp);
        }
    }

    private void handleSuspend(String streamName, StreamState state) {
        if (state.lifecycleState() != StreamLifecycleState.RUNNING) {
            throw new IllegalStateException(
                    "Cannot suspend stream " + streamName + ": current state is " + state.lifecycleState());
        }
        StreamState suspended = state.withLifecycleState(StreamLifecycleState.SUSPENDED)
                .toBuilder().epoch(Math.incrementExact(state.epoch())).build();
        stateStore().saveStateIfEpoch(suspended, state.epoch());
        LOG.info("event=stream_suspended stream={}", streamName);
    }

    private void handleResume(String streamName, StreamState state) {
        if (state.lifecycleState() != StreamLifecycleState.SUSPENDED
                && state.lifecycleState() != StreamLifecycleState.FAILED) {
            throw new IllegalStateException(
                    "Cannot resume stream " + streamName + ": current state is " + state.lifecycleState());
        }
        StreamState running = state.toBuilder()
                .lifecycleState(StreamLifecycleState.RUNNING)
                .epoch(Math.incrementExact(state.epoch()))
                .errorMessage(null)
                .updatedAt(Instant.now())
                .build();
        stateStore().saveStateIfEpoch(running, state.epoch());
        LOG.info("event=stream_resumed stream={}", streamName);
    }

    private void handleSetProperties(String streamName, StreamState state,
                                      AlterStreamAction.SetProperties sp) {
        if (state.lifecycleState() == StreamLifecycleState.DROPPING
                || state.lifecycleState() == StreamLifecycleState.DROPPED) {
            throw new IllegalStateException("Cannot configure a dropping or dropped stream: " + streamName);
        }
        Map<String, String> merged = new HashMap<>(state.streamProperties());
        merged.putAll(sp.properties());
        StreamState updated = state.toBuilder()
                .streamProperties(merged)
                .epoch(Math.incrementExact(state.epoch()))
                .updatedAt(Instant.now())
                .build();
        stateStore().saveStateIfEpoch(updated, state.epoch());
        LOG.info("event=stream_properties_updated stream={} keys={}", streamName, sp.properties().keySet());
    }

    // ── DROP ────────────────────────────────────────────────────────

    /**
     * Drops an ingestion stream.
     *
     * <p>Fences the running revision before removing the stream and its offsets.</p>
     *
     * @param stmt     parsed DROP STREAM statement
     * @param tenantId owning tenant identifier
     */
    public void drop(DropStreamStatement stmt, String tenantId) {
        String streamName = extractName(stmt.name());
        Optional<StreamState> loaded = stateStore().loadState(streamName);

        if (loaded.isEmpty()) {
            if (stmt.ifExists()) {
                LOG.info("event=stream_drop_skipped stream={} reason=not_found", streamName);
                return;
            }
            throw new StreamNotFoundException(
                    PangrovaErrorCode.STREAM_NOT_FOUND,
                    "Stream not found: " + streamName);
        }

        StreamState state = loaded.get();
        verifyTenant(state, tenantId, "drop");

        // Transition to DROPPING
        StreamState dropping = state.withLifecycleState(StreamLifecycleState.DROPPING)
                .toBuilder().epoch(Math.incrementExact(state.epoch())).build();
        stateStore().saveStateIfEpoch(dropping, state.epoch());
        LOG.info("event=stream_dropping stream={}", streamName);

        // The guarded delete cascades to cached offsets in the same transaction.
        // A separate name-only delete could erase a concurrently recreated stream's offsets.
        if (!stateStore().delete(dropping)) {
            throw new org.pangrova.common.exception.StaleEpochException(streamName, dropping.epoch());
        }

        LOG.info("event=stream_dropped stream={} tenant={}", streamName, tenantId);
    }

    // ── SHOW ────────────────────────────────────────────────────────

    /**
     * Lists all streams for the given tenant.
     *
     * @param tenantId tenant identifier
     * @return unmodifiable list of stream states
     */
    public List<StreamState> show(String tenantId) {
        return stateStore().listByTenant(tenantId);
    }

    // ── DESCRIBE ────────────────────────────────────────────────────

    /**
     * Describes a single stream by name.
     *
     * @param streamName stream name
     * @param tenantId   owning tenant identifier
     * @return the stream state, or empty if not found
     */
    public Optional<StreamState> describe(String streamName, String tenantId) {
        return stateStore().loadState(streamName)
                .filter(s -> s.tenantId().equals(tenantId));
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private StreamState loadOrThrow(String streamName, String tenantId) {
        StreamState state = stateStore().loadState(streamName)
                .orElseThrow(() -> new StreamNotFoundException(
                        PangrovaErrorCode.STREAM_NOT_FOUND,
                        "Stream not found: " + streamName));
        verifyTenant(state, tenantId, "alter");
        return state;
    }

    private static void verifyTenant(StreamState state, String tenantId, String operation) {
        if (!state.tenantId().equals(tenantId)) {
            throw new StreamNotFoundException(
                    PangrovaErrorCode.STREAM_NOT_FOUND,
                    "Stream not found: " + state.streamName());
        }
    }

    private static String extractName(org.pangrova.parser.api.ast.Identifier id) {
        List<String> parts = id.parts();
        return parts.get(parts.size() - 1);
    }

    private StreamStateStore stateStore() {
        return deps.stateStore();
    }
}
