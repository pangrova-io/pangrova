package org.pangrova.query.api.session;

import org.pangrova.connector.api.metadata.table.SnapshotRange;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.observability.api.tracing.ExecutionTraceContext;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable engine query session. Carries an authenticated actor, an optional exact
 * issued query lifetime, provider-owned correlation and trusted source selectors.
 * Client properties never grant authority or set trusted internal-execution state.
 * Copies borrow the same authority and trace snapshot; they do not close either.
 * Construction requires explicit correlation and never discovers a provider.
 */
@org.pangrova.common.api.annotation.Experimental
public record QuerySession(
        String queryId,
        Map<String, String> sessionProperties,
        UserIdentity userIdentity,
        String tenantId,
        Optional<ConnectorQueryAuthority> queryAuthority,
        ExecutionTraceContext traceContext,
        Map<String, Long> sourceSnapshots,
        Map<String, SnapshotRange> sourceIncrementalRanges,
        boolean internalMaterializationExecution,
        OptionalLong publicationRevision
) {

    public QuerySession {
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
        publicationRevision = checkNotNull(publicationRevision, "publicationRevision");
        if (publicationRevision.isPresent() && (publicationRevision.getAsLong() < 0 || queryAuthority.isEmpty())) {
            throw new IllegalArgumentException("Publication revision requires a valid admitted query");
        }
        queryAuthority.ifPresent(authority -> {
            if (!queryId.equals(authority.queryId())) {
                throw new IllegalArgumentException("Session queryId differs from its issued authority");
            }
        });
    }

    /** Creates a context without trusted source selectors. */
    public QuerySession(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId, Optional<ConnectorQueryAuthority> queryAuthority,
            ExecutionTraceContext traceContext) {
        this(queryId, sessionProperties, userIdentity, tenantId, queryAuthority, traceContext,
                Map.of(), Map.of(), false, OptionalLong.empty());
    }

    public QuerySession withTraceContext(ExecutionTraceContext context) {
        return new QuerySession(
                queryId, sessionProperties, userIdentity, tenantId, queryAuthority, context,
                sourceSnapshots, sourceIncrementalRanges, internalMaterializationExecution, publicationRevision);
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
    public QuerySession withSessionProperties(Map<String, String> properties) {
        return new QuerySession(queryId, properties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, publicationRevision);
    }

    /** Replaces the resolved tenant, dropping any revision for a different tenant. */
    public QuerySession withTenantId(String resolvedTenantId) {
        return new QuerySession(queryId, sessionProperties, userIdentity, resolvedTenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, Objects.equals(tenantId, resolvedTenantId)
                        ? publicationRevision : OptionalLong.empty());
    }

    /** Binds the tenant publication revision returned by execution admission. */
    public QuerySession withPublicationRevision(OptionalLong revision) {
        return new QuerySession(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, revision);
    }

    /** Binds the actual engine-owned query lifetime without altering the authenticated actor. */
    public QuerySession withQueryAuthority(ConnectorQueryAuthority authority) {
        if (queryAuthority.isPresent() && queryAuthority.orElseThrow() != authority) {
            throw new IllegalStateException("Session already belongs to another query lifetime");
        }
        return new QuerySession(queryId, sessionProperties, userIdentity, tenantId,
                Optional.of(authority), traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, publicationRevision);
    }

    /**
     * Returns a copy with a different queryId, preserving all other fields.
     */
    public QuerySession withQueryId(String newQueryId) {
        if (queryAuthority.isPresent() && !queryId.equals(newQueryId)) {
            throw new IllegalStateException("An admitted session cannot change its query identity");
        }
        return new QuerySession(newQueryId, sessionProperties, userIdentity, tenantId, queryAuthority,
                traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, publicationRevision);
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
    public QuerySession withSourceSnapshots(Map<String, Long> snapshots) {
        return new QuerySession(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, snapshots, sourceIncrementalRanges,
                internalMaterializationExecution, publicationRevision);
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
    public QuerySession withSourceIncrementalRanges(Map<String, SnapshotRange> ranges) {
        return new QuerySession(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, ranges,
                internalMaterializationExecution, publicationRevision);
    }

    /** Binds the durable definer identity under which kernel-authored execution runs. */
    public QuerySession withUserIdentity(UserIdentity identity) {
        return new QuerySession(queryId, sessionProperties,
                checkNotNull(identity, "identity must not be null"), tenantId, queryAuthority,
                traceContext, sourceSnapshots, sourceIncrementalRanges,
                internalMaterializationExecution, publicationRevision);
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
    public QuerySession withInternalMaterializationExecution() {
        return new QuerySession(queryId, sessionProperties, userIdentity, tenantId,
                queryAuthority, traceContext, sourceSnapshots, sourceIncrementalRanges, true, publicationRevision);
    }
}
