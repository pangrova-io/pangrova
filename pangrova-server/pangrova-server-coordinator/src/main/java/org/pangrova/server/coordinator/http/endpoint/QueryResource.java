package org.pangrova.server.coordinator.http.endpoint;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.query.api.enums.QueryState;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.server.coordinator.http.request.QueryRequestFingerprint;
import org.pangrova.server.coordinator.http.request.QueryStatusWaits;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.observability.profiling.ProfilerEndpoint;
import org.pangrova.query.profiling.QueryProfileService;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.server.coordinator.http.endpoint.QueryPeerRouting.Observation;
import org.pangrova.server.coordinator.http.endpoint.QueryPeerRouting.Operation;

import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.Optional;
import java.util.UUID;

/**
 * JAX-RS resource for query lifecycle operations.
 * Delegates to {@link QueryManager} and {@link ProfilerEndpoint}.
 *
 * <p>Responses use the shared HTTP client DTOs.
 */
@Path("/v1/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class QueryResource {
    private final ExecutionTraceContextFactory traceContexts;

    private static final int HTTP_STATUS_ACCEPTED = 202;
    private static final int OBSERVER_UUID_LENGTH = 36;

    private final QueryManager queryManager;
    private final ProfilerEndpoint profilerEndpoint;
    private final QueryProfileService queryProfileService;
    private final QueryStatusWaits statusWaits;
    private final Supplier<QueryOwnerEndpoint> ownerEndpoint;
    private final QueryPeerRouting queryPeers;

    /** Creates a resource wiring query lifecycle, profiling, and idempotency components. */
    public QueryResource(QueryManager queryManager, ProfilerEndpoint profilerEndpoint,
                         QueryProfileService queryProfileService,
                         ExecutionTraceContextFactory traceContexts, QueryStatusWaits statusWaits,
                         Supplier<QueryOwnerEndpoint> ownerEndpoint, QueryPeerRouting queryPeers) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.profilerEndpoint = profilerEndpoint;
        this.queryProfileService = queryProfileService;
        this.statusWaits = java.util.Objects.requireNonNull(statusWaits, "statusWaits");
        this.ownerEndpoint = java.util.Objects.requireNonNull(ownerEndpoint, "ownerEndpoint");
        this.queryPeers = queryPeers;
    }

    /** Submits a new query, deduplicating retries when an Idempotency-Key is supplied. */
    @POST
    @Path("/submit")
    public void submitQuery(SubmitQueryRequest request, @Context ContainerRequestContext requestContext,
            @Suspended AsyncResponse response) {
        validateSubmitRequest(request);
        UserIdentity identity = extractUserIdentity(requestContext);
        if (identity == null || identity.principal() == null || identity.principal().isBlank()) {
            throw new AuthenticationRequiredException("Query submission requires a principal");
        }
        String key = requestContext.getHeaderString("Idempotency-Key");
        String queryId = UUID.randomUUID().toString();
        QueryOwnerEndpoint localOwner = ownerEndpoint.get();
        if (queryPeers != null) {
            localOwner = queryPeers.issue(localOwner, queryId, identity.principal());
        }
        String keyDigest = key == null || key.isBlank()
                ? QueryRequestFingerprint.freshKeyDigest(queryId) : QueryRequestFingerprint.keyDigest(key);
        var acceptance = new QueryAcceptanceRequest(identity.principal(),
                keyDigest, QueryRequestFingerprint.request(request), localOwner);
        QuerySession session = new QuerySession(
                queryId, request.sessionProperties(), identity, null, Optional.empty(), traceContexts.capture());
        queryManager.submitQueryAsync(queryId, request.query(), session, acceptance)
                .thenApply(QueryResponseMapper::submission)
                .whenComplete((result, failure) -> {
                    if (failure == null) response.resume(Response.ok(result).build());
                    else response.resume(unwrap(failure));
                });
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
    }

    private static UserIdentity extractUserIdentity(ContainerRequestContext ctx) {
        if (ctx == null) {
            return null;
        }
        Object prop = ctx.getProperty(AuthenticationFilter.USER_IDENTITY_PROPERTY);
        return (prop instanceof UserIdentity uid) ? uid : null;
    }

    /** Reads the owner projection or suspends an observer until its selected event or deadline. */
    @GET
    @Path("/{queryId}/state")
    public void getQueryState(@PathParam("queryId") String queryId,
                              @QueryParam("afterVersion") @DefaultValue("-1") long afterVersion,
                              @QueryParam("waitMs") @DefaultValue("0") long waitMillis,
                              @QueryParam("until") @DefaultValue("NEXT_VERSION") QueryWaitTarget target,
                              @QueryParam("history") @DefaultValue("false") boolean history,
                              @HeaderParam("X-Pangrova-Query-Observer") String observer,
                              @Context ContainerRequestContext requestContext,
                              @Suspended AsyncResponse response) {
        var observation = new Observation(afterVersion, waitMillis, target, observer);
        validateObservation(observation, history);
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        QueryStateMachine owner = queryManager.getStateMachine(queryId);
        if (history) {
            response.resume(owner != null && !owner.isOwnedBy(principal)
                    ? queryNotFound(queryId) : historicalStatus(queryId, principal));
            return;
        }
        if (owner == null && relay(queryId, principal, Operation.STATE, observation, requestContext, response)) {
            return;
        }
        observe(queryId, principal, observation, owner, response);
    }

    /** Returns the original authenticated receipt outcome after durable physical retirement. */
    @GET
    @Path("/{queryId}/receipt-recovery")
    public Response recoverReceipt(@PathParam("queryId") String queryId,
            @HeaderParam(QueryOwnerEndpoint.ROUTE_HEADER) String ticket,
            @Context ContainerRequestContext requestContext) {
        UserIdentity identity = extractUserIdentity(requestContext);
        if (identity == null || queryPeers == null
                || !queryPeers.authenticatesReceipt(ticket, queryId, identity.principal())) {
            return queryNotFound(queryId);
        }
        var recovery = queryManager.recoverReceipt(queryId, identity.principal(), ticket);
        if (recovery.isEmpty()) return queryNotFound(queryId);
        var receipt = recovery.orElseThrow();
        if (receipt.pending()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorDetail("QUERY_RECOVERY_PENDING", null, "Query retirement is still pending"))
                    .build();
        }
        var outcome = receipt.outcome();
        return Response.ok(QueryStatusResponse.recovered(queryId, receipt.terminalState(),
                QueryResponseMapper.failure(outcome.error()), outcome.resultDisposition(),
                QueryResponseMapper.branchSelection(outcome.branchSelection()))).build();
    }

    private static void validateObservation(Observation observation, boolean history) {
        long afterVersion = observation.afterVersion();
        long waitMillis = observation.waitMillis();
        String observer = observation.observer();
        if (afterVersion < -1 || waitMillis < 0 || waitMillis > QueryStatusResponse.MAX_WAIT_MILLIS
                || observation.target() == null
                || (waitMillis > 0 && (afterVersion < 0 || observer == null
                        || observer.length() != OBSERVER_UUID_LENGTH))
                || (history && (afterVersion != -1 || waitMillis != 0
                        || observation.target() != QueryWaitTarget.NEXT_VERSION))) {
            throw new IllegalArgumentException("Invalid query status wait parameters");
        }
    }

    void observeLocal(String queryId, String principal, Observation observation, AsyncResponse response) {
        validateObservation(observation, false);
        observe(queryId, principal, observation, queryManager.getStateMachine(queryId), response);
    }

    private void observe(String queryId, String principal, Observation observation, QueryStateMachine owner,
            AsyncResponse response) {
        if (owner == null) {
            response.resume(observation.afterVersion() >= 0 ? ownerUnavailable() : queryNotFound(queryId));
            return;
        }
        if (!owner.isOwnedBy(principal)) {
            response.resume(queryNotFound(queryId));
            return;
        }
        var current = queryManager.observeOwnerStatus(queryId, owner);
        if (observation.waitMillis() == 0 || current.reaches(observation.target(), observation.afterVersion())) {
            response.resume(statusResponse(queryId, current));
            return;
        }
        statusWaits.suspend(queryId, principal, observation.observer(), owner, observation.target(),
                observation.afterVersion(), observation.waitMillis(), response,
                status -> statusResponse(queryId, status));
    }

    boolean relay(String queryId, String principal, Operation operation, Observation observation,
            ContainerRequestContext requestContext, AsyncResponse response) {
        if (queryPeers == null || extractUserIdentity(requestContext) == null) return false;
        String ticket = requestContext.getHeaderString(QueryOwnerEndpoint.ROUTE_HEADER);
        return ticket != null && queryPeers.forward(ticket, queryId, principal, ownerEndpoint.get(),
                operation, observation, response);
    }

    private Response historicalStatus(String queryId, String principal) {
        return queryManager.getHistoricalQueryState(queryId, principal)
                .map(state -> Response.ok(QueryStatusResponse.historical(queryId, state.name())).build())
                .orElseGet(() -> queryNotFound(queryId));
    }

    private static Response ownerUnavailable() {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new ErrorDetail("QUERY_OWNER_UNAVAILABLE", null,
                        "Live query owner is unavailable on this coordinator"))
                .build();
    }

    private static Response queryNotFound(String queryId) {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new ErrorDetail("NOT_FOUND", null, "Query not found: " + queryId)).build();
    }

    private static Response statusResponse(String queryId, QueryStateMachine.Status status) {
        ErrorDetail error = status.error() == null ? null : QueryResponseMapper.failure(status.error());
        return Response.ok(new QueryStatusResponse(queryId, status.state().name(),
                error, null, status.disposition(), status.endpoint(), status.version(), status.drained(),
                QueryResponseMapper.branchSelection(status.branchSelection()))).build();
    }

    /** Requests cancellation of a running query. */
    @POST
    @Path("/{queryId}/cancel")
    public void cancelQuery(@PathParam("queryId") String queryId,
            @Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        var owner = queryManager.getStateMachine(queryId);
        if (owner == null && relay(queryId, principal, Operation.CANCEL, null, requestContext, response)) return;
        response.resume(cancelLocal(queryId, principal, owner));
    }

    Response cancelLocal(String queryId, String principal) {
        return cancelLocal(queryId, principal, queryManager.getStateMachine(queryId));
    }

    private Response cancelLocal(String queryId, String principal, QueryStateMachine owner) {
        if (owner == null || !owner.isOwnedBy(principal)) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new CancelResponse(queryId, false, "NOT_FOUND"))
                    .build();
        }
        if (isTerminalState(owner.status().state())) {
            return Response.ok(new CancelResponse(queryId, false, "ALREADY_TERMINAL"))
                    .build();
        }
        boolean cancelled = queryManager.cancelQuery(queryId);
        if (cancelled) {
            return Response.status(HTTP_STATUS_ACCEPTED)
                    .entity(new CancelResponse(queryId, true, "CANCEL_ACCEPTED"))
                    .build();
        }
        return Response.ok(new CancelResponse(queryId, false, "ALREADY_TERMINAL"))
                .build();
    }

    /** Returns the flame-graph profile of a query as SVG. */
    @GET
    @Path("/profile/{queryId}")
    @Produces("image/svg+xml")
    public void getProfile(@PathParam("queryId") String queryId,
            @Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        var owner = queryManager.getStateMachine(queryId);
        if (owner == null
                && relay(queryId, principal, Operation.FLAME_GRAPH, null, requestContext, response)) {
            return;
        }
        response.resume(flameGraph(queryId, principal, owner));
    }

    Response flameGraphLocal(String queryId, String principal) {
        return flameGraph(queryId, principal, queryManager.getStateMachine(queryId));
    }

    private Response flameGraph(String queryId, String principal, QueryStateMachine owner) {
        if (profilerEndpoint == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND", "Profiler not available"))
                    .build();
        }
        if (owner != null && !owner.isOwnedBy(principal)) {
            return profileNotFound(queryId);
        }
        byte[] flameGraphData;
        try {
            flameGraphData = profilerEndpoint.getFlameGraph(queryId);
        } catch (org.pangrova.observability.api.exception.ProfilingUnavailableException unavailable) {
            return Response.status(Response.Status.NOT_IMPLEMENTED)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorDetail("UNSUPPORTED_CAPABILITY", "UNSUPPORTED_CAPABILITY",
                            unavailable.getMessage()))
                    .build();
        }
        if (flameGraphData == null || flameGraphData.length == 0 || owner == null) {
            return profileNotFound(queryId);
        }
        return Response.ok(flameGraphData, "image/svg+xml").build();
    }

    /** Returns the structured JSON profile of a query. */
    @GET
    @Path("/profile/{queryId}/json")
    @Produces(MediaType.APPLICATION_JSON)
    public void getProfileJson(@PathParam("queryId") String queryId,
            @QueryParam("history") @DefaultValue("false") boolean history,
            @Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        var owner = queryManager.getStateMachine(queryId);
        if (!history && owner == null
                && relay(queryId, principal, Operation.PROFILE, null, requestContext, response)) {
            return;
        }
        response.resume(jsonProfile(queryId, principal, history, owner));
    }

    Response jsonProfileLocal(String queryId, String principal) {
        return jsonProfile(queryId, principal, false, queryManager.getStateMachine(queryId));
    }

    private Response jsonProfile(String queryId, String principal, boolean history, QueryStateMachine owner) {
        if (queryProfileService == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND",
                            "Query profile service not available"))
                    .build();
        }
        return queryProfileService.getVisibleProfile(queryId, principal, owner, history)
                .map(profile -> Response.ok(profile).build()).orElseGet(() -> profileNotFound(queryId));
    }

    private static Response profileNotFound(String queryId) {
        return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
                .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND", "No profile data for query: " + queryId))
                .build();
    }

    private static void validateSubmitRequest(SubmitQueryRequest request) {
        if (request == null || request.query() == null || request.query().isBlank()) {
            throw new IllegalArgumentException(
                    "query field is required and must not be blank");
        }
    }

    record CancelResponse(String query_id, boolean cancelled, String outcome) {}

    private static boolean isTerminalState(QueryState state) {
        return state == QueryState.FINISHED
                || state == QueryState.FAILED
                || state == QueryState.CANCELLED;
    }
}
