package org.pangrova.coordinator.http.endpoint;

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
import org.pangrova.coordinator.http.IdempotencyKeyRegistry;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.spi.observability.profiling.ProfilerEndpoint;
import org.pangrova.query.core.profiling.QueryProfileService;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.client.core.protocol.QueryStatusResponse;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.client.core.protocol.SubmitQueryRequest;
import org.pangrova.client.core.protocol.SubmitQueryResponse;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.spi.security.UserIdentity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JAX-RS resource for query lifecycle operations.
 * Delegates to {@link QueryManager} and {@link ProfilerEndpoint}.
 *
 * <p>All responses use strongly-typed DTOs from {@code pangrova-protocol}
 * for compile-time type safety and stable JSON contract.
 */
@Path("/v1/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class QueryResource {

    private static final int HTTP_STATUS_ACCEPTED = 202;

    private final QueryManager queryManager;
    private final ProfilerEndpoint profilerEndpoint;
    private final QueryProfileService queryProfileService;
    private final IdempotencyKeyRegistry idempotencyRegistry;

    /** Creates a resource wiring query lifecycle, profiling, and idempotency components. */
    public QueryResource(QueryManager queryManager, ProfilerEndpoint profilerEndpoint,
                         QueryProfileService queryProfileService,
                         IdempotencyKeyRegistry idempotencyRegistry) {
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
        // A-CLIENT-01: if the client supplied an Idempotency-Key, dedup retries so a retried write
        // resolves to the SAME queryId instead of executing twice. Without a key, behave as before.
        String idempotencyKey = requestContext != null
                ? requestContext.getHeaderString("Idempotency-Key") : null;
        if (idempotencyRegistry != null && idempotencyKey != null && !idempotencyKey.isBlank()) {
            IdempotencyKeyRegistry.Resolution resolution =
                    idempotencyRegistry.resolve(idempotencyKey, () -> UUID.randomUUID().toString());
            if (resolution.duplicate()) {
                String state = queryManager.getQueryState(resolution.queryId())
                        .map(Enum::name).orElse("SUBMITTED");
                ResultEndpoint endpoint =
                        queryManager.getResultEndpoint(resolution.queryId()).orElse(null);
                return Response.ok(new SubmitQueryResponse(
                        resolution.queryId(), state, endpoint, List.of())).build();
            }
            return doSubmit(resolution.queryId(), request, requestContext);
        }
        return doSubmit(UUID.randomUUID().toString(), request, requestContext);
    }

    private Response doSubmit(String queryId, SubmitQueryRequest request,
                              ContainerRequestContext requestContext) {
        UserIdentity identity = extractUserIdentity(requestContext);
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                queryId, request.sessionProperties(), identity, null);
        SubmitQueryResponse response = queryManager.submitQuery(
                queryId, request.query(), session);
        return Response.ok(response).build();
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
        Optional<QueryStateMachine.State> state =
                queryManager.getQueryStateWithJournalFallback(queryId);
        if (state.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", null,
                            "Query not found: " + queryId))
                    .build();
        }
        ResultEndpoint endpoint = queryManager.getResultEndpoint(queryId).orElse(null);
        ErrorDetail error = queryManager.getQueryError(queryId).orElse(null);
        QueryStatusResponse body = QueryStatusResponse.of(
                queryId, state.get().name(), error, endpoint);
        return Response.ok(body).build();
    }

    /** Requests cancellation of a running query. */
    @POST
    @Path("/{queryId}/cancel")
    public Response cancelQuery(@PathParam("queryId") String queryId) {
        Optional<QueryStateMachine.State> state = queryManager.getQueryState(queryId);
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
        byte[] flameGraphData = profilerEndpoint.getFlameGraph(queryId);
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
        if (request.query() == null || request.query().isBlank()) {
            throw new IllegalArgumentException(
                    "query field is required and must not be blank");
        }
    }

    record CancelResponse(String query_id, boolean cancelled, String outcome) {}

    private static boolean isTerminalState(QueryStateMachine.State state) {
        return state == QueryStateMachine.State.FINISHED
                || state == QueryStateMachine.State.FAILED
                || state == QueryStateMachine.State.CANCELLED;
    }
}
