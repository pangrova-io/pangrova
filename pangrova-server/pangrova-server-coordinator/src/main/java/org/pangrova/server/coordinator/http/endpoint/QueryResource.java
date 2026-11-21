package org.pangrova.server.coordinator.http.endpoint;

import org.pangrova.server.coordinator.exception.RecordedSubmissionException;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.query.api.enums.QueryState;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.server.coordinator.http.request.IdempotencyKeyRegistry;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.observability.profiling.ProfilerEndpoint;
import org.pangrova.query.profiling.QueryProfileService;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.api.identity.UserIdentity;

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

    private final QueryManager queryManager;
    private final ProfilerEndpoint profilerEndpoint;
    private final QueryProfileService queryProfileService;
    private final IdempotencyKeyRegistry idempotencyRegistry;

    /** Creates a resource wiring query lifecycle, profiling, and idempotency components. */
    public QueryResource(QueryManager queryManager, ProfilerEndpoint profilerEndpoint,
                         QueryProfileService queryProfileService,
                         IdempotencyKeyRegistry idempotencyRegistry, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.profilerEndpoint = profilerEndpoint;
        this.queryProfileService = queryProfileService;
        this.idempotencyRegistry = idempotencyRegistry;
    }

    /** Submits a new query, deduplicating retries when an Idempotency-Key is supplied. */
    @POST
    @Path("/submit")
    public Response submitQuery(SubmitQueryRequest request,
                                @Context ContainerRequestContext requestContext) {
        validateSubmitRequest(request);
        UserIdentity identity = extractUserIdentity(requestContext);
        String key = requestContext == null ? null : requestContext.getHeaderString("Idempotency-Key");
        SubmitQueryResponse receipt;
        if (idempotencyRegistry != null && key != null && !key.isBlank()) {
            if (identity == null || identity.principal() == null || identity.principal().isBlank()) {
                throw new AuthenticationRequiredException("Idempotent query submission requires a principal");
            }
            receipt = idempotencyRegistry.submit(identity.principal(), key, request, () -> submitWithIdempotency(
                    request, identity));
        } else {
            receipt = doSubmit(UUID.randomUUID().toString(), request, identity);
        }
        return Response.ok(receipt).build();
    }

    private SubmitQueryResponse submitWithIdempotency(SubmitQueryRequest request, UserIdentity identity) {
        String queryId = UUID.randomUUID().toString();
        try {
            return doSubmit(queryId, request, identity);
        } catch (RuntimeException failure) {
            boolean recorded;
            try {
                recorded = queryManager.getQueryStateWithJournalFallback(queryId).isPresent();
            } catch (RuntimeException checkFailure) {
                failure.addSuppressed(checkFailure);
                recorded = true;
            }
            if (recorded) {
                throw new RecordedSubmissionException(queryId, failure);
            }
            throw failure;
        }
    }

    private SubmitQueryResponse doSubmit(String queryId, SubmitQueryRequest request, UserIdentity identity) {
        QuerySession session = new QuerySession(
                queryId, request.sessionProperties(), identity, null, Optional.empty(), traceContexts.capture());
        return QueryResponseMapper.submission(queryManager.submitQuery(queryId, request.query(), session));
    }

    private static UserIdentity extractUserIdentity(ContainerRequestContext ctx) {
        if (ctx == null) {
            return null;
        }
        Object prop = ctx.getProperty(AuthenticationFilter.USER_IDENTITY_PROPERTY);
        return (prop instanceof UserIdentity uid) ? uid : null;
    }

    /** Returns the current state of a query, with journal fallback for peer-submitted queries. */
    @GET
    @Path("/{queryId}/state")
    public Response getQueryState(@PathParam("queryId") String queryId) {
        // A-XNODE-01: journal fallback lets any coordinator in an Active-Active
        // cluster answer state polls for queries accepted by a peer node.
        Optional<QueryState> state =
                queryManager.getQueryStateWithJournalFallback(queryId);
        if (state.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", null,
                            "Query not found: " + queryId))
                    .build();
        }
        ResultEndpoint endpoint = queryManager.getResultEndpoint(queryId).orElse(null);
        ErrorDetail error = queryManager.getQueryError(queryId).map(QueryResponseMapper::failure).orElse(null);
        QueryStatusResponse body = QueryStatusResponse.of(
                queryId, state.get().name(), error, endpoint);
        return Response.ok(body).build();
    }

    /** Requests cancellation of a running query. */
    @POST
    @Path("/{queryId}/cancel")
    public Response cancelQuery(@PathParam("queryId") String queryId) {
        Optional<QueryState> state = queryManager.getQueryState(queryId);
        if (state.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new CancelResponse(queryId, false, "NOT_FOUND"))
                    .build();
        }
        if (isTerminalState(state.get())) {
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
    public Response getProfile(@PathParam("queryId") String queryId) {
        if (profilerEndpoint == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND", "Profiler not available"))
                    .build();
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
        if (flameGraphData == null || flameGraphData.length == 0) {
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND",
                            "No profile data for query: " + queryId))
                    .build();
        }
        return Response.ok(flameGraphData, "image/svg+xml").build();
    }

    /** Returns the structured JSON profile of a query. */
    @GET
    @Path("/profile/{queryId}/json")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getProfileJson(@PathParam("queryId") String queryId) {
        if (queryProfileService == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND",
                            "Query profile service not available"))
                    .build();
        }
        var profile = queryProfileService.getProfile(queryId);
        if (profile.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", "NOT_FOUND",
                            "No profile data for query: " + queryId))
                    .build();
        }
        return Response.ok(profile.get()).build();
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
