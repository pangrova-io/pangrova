package org.pangrova.coordinator.query.session;

import org.pangrova.connector.spi.metadata.table.SnapshotRange;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.connector.spi.ConnectorQueryAuthority;
import org.pangrova.observability.sdk.tracing.ExecutionTraceContext;

import java.util.Map;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Minimal value object carrying query context through the orchestration pipeline.
 * Distinct from SPI SessionContext — this is coordinator-internal.
 */
public record CoordinatorSessionContext(
        String queryId,
        Map<String, String> sessionProperties,
        UserIdentity userIdentity,
        String tenantId,
        Optional<ConnectorQueryAuthority> queryAuthority,
        ExecutionTraceContext traceContext,
        Map<String, Long> sourceSnapshots,
        Map<String, SnapshotRange> sourceIncrementalRanges,
        boolean internalMaterializationExecution
) {

    public CoordinatorSessionContext {
        checkNotNull(queryId, "queryId must not be null");
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
        sourceSnapshots = sourceSnapshots == null ? Map.of() : Map.copyOf(sourceSnapshots);
        sourceIncrementalRanges = sourceIncrementalRanges == null
                ? Map.of() : Map.copyOf(sourceIncrementalRanges);
        for (String table : sourceIncrementalRanges.keySet()) {
            if (sourceSnapshots.containsKey(table)) {
                throw new IllegalArgumentException(
                        "A source cannot bind both an exact snapshot and an incremental range: " + table);
            }
        }
        queryAuthority = checkNotNull(queryAuthority, "queryAuthority must not be null");
        traceContext = checkNotNull(traceContext, "traceContext must not be null");
        queryAuthority.ifPresent(authority -> {
            if (!queryId.equals(authority.queryId())) {
                throw new IllegalArgumentException("Session queryId differs from its issued authority");
            }
        });
    }

    /** Creates a context without trusted source selectors. */
    public CoordinatorSessionContext(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId, Optional<ConnectorQueryAuthority> queryAuthority,
            ExecutionTraceContext traceContext) {
        this(queryId, sessionProperties, userIdentity, tenantId, queryAuthority, traceContext,
                Map.of(), Map.of(), false);
    }

    /** Creates an authenticated request context before engine query admission. */
    public CoordinatorSessionContext(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId) {
        this(queryId, sessionProperties, userIdentity, tenantId, Optional.empty(), ExecutionTraceContext.capture());
    }

    public CoordinatorSessionContext(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId, Optional<ConnectorQueryAuthority> queryAuthority) {
        this(queryId, sessionProperties, userIdentity, tenantId, queryAuthority, ExecutionTraceContext.capture());
    }

    public CoordinatorSessionContext withTraceContext(ExecutionTraceContext context) {
        return new CoordinatorSessionContext(
                queryId, sessionProperties, userIdentity, tenantId, queryAuthority, context,
                sourceSnapshots, sourceIncrementalRanges, internalMaterializationExecution);
    }

    /**
     * Replaces session properties while preserving trusted engine state.
     *
     * <p>Source selectors and the internal-execution marker are kernel-issued;
     * rebuilding a session for property enrichment must never drop them.
     *
     * @param properties replacement session properties
     * @return copy carrying the same trusted state
     */
    public CoordinatorSessionContext withSessionProperties(Map<String, String> properties) {
        return new CoordinatorSessionContext(queryId, properties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution);
    }

    /** Binds the actual engine-owned query lifetime without altering the authenticated actor. */
    public CoordinatorSessionContext withQueryAuthority(ConnectorQueryAuthority authority) {
        if (queryAuthority.isPresent() && queryAuthority.orElseThrow() != authority) {
            throw new IllegalStateException("Session already belongs to another query lifetime");
        }
        return new CoordinatorSessionContext(queryId, sessionProperties, userIdentity, tenantId,
                Optional.of(authority), traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution);
    }

    /**
     * Returns a copy with a different queryId, preserving all other fields.
     */
    public CoordinatorSessionContext withQueryId(String newQueryId) {
        if (queryAuthority.isPresent() && !queryId.equals(newQueryId)) {
            throw new IllegalStateException("An admitted session cannot change its query identity");
        }
        return new CoordinatorSessionContext(newQueryId, sessionProperties, userIdentity, tenantId, queryAuthority,
                traceContext, sourceSnapshots, sourceIncrementalRanges, internalMaterializationExecution);
    }

    /**
     * Binds exact source snapshots resolved by the materialization kernel.
     *
     * <p>These selectors never come from client session properties: they are
     * derived from the attempt's pinned source vector, so execution reads the
     * same immutable snapshots that were planned and held.
     *
     * @param snapshots canonical {@code catalog.schema.table} to snapshot id
     * @return copy carrying the trusted selectors
     */
    public CoordinatorSessionContext withSourceSnapshots(Map<String, Long> snapshots) {
        return new CoordinatorSessionContext(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, snapshots, sourceIncrementalRanges,
                internalMaterializationExecution);
    }

    /**
     * Binds incremental snapshot ranges resolved by the materialization kernel.
     *
     * <p>Like exact source snapshots, these selectors are trusted kernel output
     * and never originate from client session properties. A source bound here
     * reads only {@code (fromExclusive, toInclusive]}, which is what makes an
     * append-only refresh read the appended delta rather than the whole table.
     *
     * @param ranges canonical {@code catalog.schema.table} to snapshot range
     * @return copy carrying the trusted selectors
     */
    public CoordinatorSessionContext withSourceIncrementalRanges(Map<String, SnapshotRange> ranges) {
        return new CoordinatorSessionContext(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, ranges,
                internalMaterializationExecution);
    }

    /** Binds the durable definer identity under which kernel-authored execution runs. */
    public CoordinatorSessionContext withUserIdentity(UserIdentity identity) {
        return new CoordinatorSessionContext(queryId, sessionProperties,
                checkNotNull(identity, "identity must not be null"), tenantId, queryAuthority,
                traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution);
    }

    /**
     * Marks this query as kernel-authored materialization execution.
     *
     * <p>Only the materialization kernel sets this after validating the write
     * capability for the attempt. It lets the fenced candidate and its pinned
     * base generation pass the planner and access-control reserved-name guards;
     * it never widens authority for ordinary client SQL, and it is not
     * derivable from client session properties.</p>
     *
     * @return copy carrying the trusted internal-execution marker
     */
    public CoordinatorSessionContext withInternalMaterializationExecution() {
        return new CoordinatorSessionContext(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges, true);
    }
}
