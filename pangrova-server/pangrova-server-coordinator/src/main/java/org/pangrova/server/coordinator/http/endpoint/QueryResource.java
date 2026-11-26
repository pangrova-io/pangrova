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
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.api.identity.UserIdentity;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
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

    /** Creates a resource wiring query lifecycle, profiling, and idempotency components. */
    public QueryResource(QueryManager queryManager, ProfilerEndpoint profilerEndpoint,
                         QueryProfileService queryProfileService,
                         ExecutionTraceContextFactory traceContexts, QueryStatusWaits statusWaits) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.profilerEndpoint = profilerEndpoint;
        this.queryProfileService = queryProfileService;
        this.statusWaits = java.util.Objects.requireNonNull(statusWaits, "statusWaits");
    }

    /** Submits a new query, deduplicating retries when an Idempotency-Key is supplied. */
    @POST
    @Path("/submit")
    public void submitQuery(SubmitQueryRequest request, @Context ContainerRequestContext requestContext,
            @Suspended AsyncResponse response) {
        validateSubmitRequest(request);
        UserIdentity identity = extractUserIdentity(requestContext);
        String key = requestContext == null ? null : requestContext.getHeaderString("Idempotency-Key");
        CompletionStage<SubmitQueryResponse> receipt;
        if (key != null && !key.isBlank()) {
            if (identity == null || identity.principal() == null || identity.principal().isBlank()) {
                throw new AuthenticationRequiredException("Idempotent query submission requires a principal");
            }
            var acceptance = new QueryAcceptanceRequest(identity.principal(),
                    QueryRequestFingerprint.keyDigest(key), QueryRequestFingerprint.request(request));
            receipt = doSubmit(UUID.randomUUID().toString(), request, identity, acceptance);
        } else {
            receipt = doSubmit(UUID.randomUUID().toString(), request, identity, null);
        }
        receipt.whenComplete((result, failure) -> {
            if (failure == null) {
                response.resume(Response.ok(result).build());
            } else {
                response.resume(unwrap(failure));
            }
        });
    }

    private CompletionStage<SubmitQueryResponse> doSubmit(
            String queryId, SubmitQueryRequest request, UserIdentity identity,
            QueryAcceptanceRequest acceptance) {
        QuerySession session = new QuerySession(
                queryId, request.sessionProperties(), identity, null, Optional.empty(), traceContexts.capture());
        return queryManager.submitQueryAsync(queryId, request.query(), session, acceptance)
                .thenApply(QueryResponseMapper::submission);
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
                              @HeaderParam("X-Pangrova-Query-Observer") String observer,
                              @Context ContainerRequestContext requestContext,
                              @Suspended AsyncResponse response) {
        if (afterVersion < -1 || waitMillis < 0 || waitMillis > QueryStatusResponse.MAX_WAIT_MILLIS
                || (waitMillis > 0 && (afterVersion < 0 || observer == null
                        || observer.length() != OBSERVER_UUID_LENGTH))) {
            throw new IllegalArgumentException("Invalid query status wait parameters");
        }
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        QueryStateMachine owner = queryManager.getStateMachine(queryId);
        if (owner == null) {
            resumeWithoutOwner(queryId, principal, afterVersion, response);
            return;
        }
        if (!owner.isOwnedBy(principal)) {
            response.resume(queryNotFound(queryId));
            return;
        }
        var current = owner.status();
        if (waitMillis == 0 || current.reaches(target, afterVersion)) {
            response.resume(statusResponse(queryId, current));
            return;
        }
        statusWaits.suspend(queryId, principal, observer,
                owner, target, afterVersion, waitMillis, response, status -> statusResponse(queryId, status));
    }

    private void resumeWithoutOwner(String queryId, String principal, long afterVersion, AsyncResponse response) {
        if (afterVersion >= 0) {
            response.resume(Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("QUERY_OWNER_UNAVAILABLE", null,
                            "Live query owner is unavailable on this coordinator"))
                    .build());
            return;
        }
        Optional<QueryState> state = queryManager.getHistoricalQueryState(queryId, principal);
        if (state.isPresent()) {
            response.resume(Response.ok(QueryStatusResponse.historical(
                    queryId, state.get().name())).build());
        } else {
            response.resume(queryNotFound(queryId));
        }
    }

    private static Response queryNotFound(String queryId) {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new ErrorDetail("NOT_FOUND", null, "Query not found: " + queryId)).build();
    }

    private static Response statusResponse(String queryId, QueryStateMachine.Status status) {
        ErrorDetail error = status.error() == null ? null : QueryResponseMapper.failure(status.error());
        return Response.ok(new QueryStatusResponse(queryId, status.state().name(),
                error, null, status.disposition(), status.endpoint(), status.version(), status.drained())).build();
    }

    /** Requests cancellation of a running query. */
    @POST
    @Path("/{queryId}/cancel")
    public Response cancelQuery(@PathParam("queryId") String queryId,
            @Context ContainerRequestContext requestContext) {
        var owner = queryManager.getStateMachine(queryId);
        if (owner == null || !owner.isOwnedBy(AuthenticationFilter.queryPrincipal(requestContext))) {
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
    public Response getProfile(@PathParam("queryId") String queryId,
            @Context ContainerRequestContext requestContext) {
        if (profilerEndpoint == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND", "Profiler not available"))
                    .build();
        }
        var owner = queryManager.getStateMachine(queryId);
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
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
    public Response getProfileJson(@PathParam("queryId") String queryId,
            @Context ContainerRequestContext requestContext) {
        if (queryProfileService == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND",
                            "Query profile service not available"))
                    .build();
        }
        var owner = queryManager.getStateMachine(queryId);
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        if (owner != null && !owner.isOwnedBy(principal)) {
            return profileNotFound(queryId);
        }
        return queryProfileService.getProfile(queryId, principal)
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
