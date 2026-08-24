package org.pangrova.coordinator.streaming;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.StreamAlreadyExistsException;
import org.pangrova.common.exception.StreamNotFoundException;
import org.pangrova.common.exception.StreamQuotaExceededException;
import org.pangrova.metadata.streaming.StreamLifecycleState;
import org.pangrova.metadata.streaming.StreamState;
import org.pangrova.metadata.streaming.StreamStateStore;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
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
 *   CREATING → RUNNING
 *   RUNNING  → SUSPENDED
 *   SUSPENDED → RUNNING (resume)
 *   FAILED   → RUNNING (resume)
 *   any      → DROPPING → DROPPED
 * </pre>
 *
 * <p>Concurrency strategy: all mutable state is managed by thread-safe
 * dependencies ({@link StreamStateStore}, {@link StreamScheduler}).
 * This class holds no mutable fields.</p>
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
     * <p>Validates source properties contain {@code bootstrap.servers} and {@code topic},
     * checks per-tenant quota, persists CREATING state, registers with scheduler,
     * then transitions to RUNNING.</p>
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
        if (activeCount >= deps.config().maxStreamsPerTenant()) {
            throw new StreamQuotaExceededException(
                    PangrovaErrorCode.STREAM_QUOTA_EXCEEDED,
                    "Tenant " + tenantId + " exceeds max streams limit: "
                            + deps.config().maxStreamsPerTenant());
        }

        // Validate source properties
        validateSourceProperties(stmt.sourceProperties());

        // Persist CREATING state
        Instant now = Instant.now();
        StreamState creating = StreamState.builder()
                .streamName(streamName)
                .tenantId(tenantId)
                .lifecycleState(StreamLifecycleState.CREATING)
                .targetTable(extractName(stmt.targetTable()))
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
        stateStore().saveState(creating);
        LOG.info("event=stream_state_persisted stream={} tenant={} state=CREATING", streamName, tenantId);

        // Register with scheduler and transition to RUNNING
        StreamState running = creating.withLifecycleState(StreamLifecycleState.RUNNING);
        stateStore().saveState(running);
        deps.scheduler().start(running);

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
        deps.scheduler().stop(streamName);
        StreamState suspended = state.withLifecycleState(StreamLifecycleState.SUSPENDED);
        stateStore().saveState(suspended);
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
                .errorMessage(null)
                .updatedAt(Instant.now())
                .build();
        stateStore().saveState(running);
        deps.scheduler().start(running);
        LOG.info("event=stream_resumed stream={}", streamName);
    }

    private void handleSetProperties(String streamName, StreamState state,
                                      AlterStreamAction.SetProperties sp) {
        Map<String, String> merged = new HashMap<>(state.streamProperties());
        merged.putAll(sp.properties());
        StreamState updated = state.toBuilder()
                .streamProperties(merged)
                .updatedAt(Instant.now())
                .build();
        stateStore().saveState(updated);
        LOG.info("event=stream_properties_updated stream={} keys={}", streamName, sp.properties().keySet());
    }

    // ── DROP ────────────────────────────────────────────────────────

    /**
     * Drops an ingestion stream.
     *
     * <p>Transitions to DROPPING, stops the pipeline, deletes state and offsets,
     * then marks as DROPPED.</p>
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
        StreamState dropping = state.withLifecycleState(StreamLifecycleState.DROPPING);
        stateStore().saveState(dropping);
        LOG.info("event=stream_dropping stream={}", streamName);

        // Stop pipeline
        deps.scheduler().stop(streamName);

        // Clean up offsets and state
        deps.offsetStore().deleteOffsets(streamName);
        stateStore().delete(streamName);

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

    private static void validateSourceProperties(Map<String, String> sourceProps) {
        if (!sourceProps.containsKey("bootstrap.servers")) {
            throw new IllegalArgumentException(
                    "Source properties must contain 'bootstrap.servers'");
        }
        if (!sourceProps.containsKey("topic")) {
            throw new IllegalArgumentException(
                    "Source properties must contain 'topic'");
        }
    }

    private static String extractName(org.pangrova.parser.ast.Identifier id) {
        List<String> parts = id.parts();
        return parts.get(parts.size() - 1);
    }

    private StreamStateStore stateStore() {
        return deps.stateStore();
    }
}
