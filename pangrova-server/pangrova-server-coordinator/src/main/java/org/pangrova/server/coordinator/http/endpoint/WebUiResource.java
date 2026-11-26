package org.pangrova.server.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.query.profiling.QueryProfileService;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST resource serving Web UI API endpoints for query list, query profile,
 * and worker status dashboard.
 *
 * <p>Provides three endpoints:
 * <ul>
 *   <li>{@code GET /api/queries} — submitting principal's active queries</li>
 *   <li>{@code GET /api/queries/{id}/profile} — the submitting principal's query profile</li>
 *   <li>{@code GET /api/workers} — worker status dashboard (health, memory, tasks)</li>
 * </ul>
 */
@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public final class WebUiResource {

    private final QueryManager queryManager;
    private final WorkerManager workerManager;
    private final QueryProfileService queryProfileService;
    private final QueryResource queries;

    /** Exposes local dashboard data and the query peer route for profile requests. */
    public WebUiResource(QueryManager queryManager, WorkerManager workerManager,
                         QueryProfileService queryProfileService, QueryResource queries) {
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.workerManager = workerManager;
        this.queryProfileService = queryProfileService;
        this.queries = queries;
    }

    /** Lists active queries submitted by the caller. */
    @GET
    @Path("/queries")
    public Response listQueries(@Context ContainerRequestContext requestContext) {
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        Map<String, QueryStateMachine> machines = queryManager.getStateMachines();
        List<QuerySummary> summaries = machines.entrySet().stream()
                .filter(entry -> entry.getValue().isOwnedBy(principal))
                .map(entry -> toQuerySummary(entry.getKey(), entry.getValue()))
                .toList();
        return Response.ok(summaries).build();
    }

    /** Returns the stored profile for a query, or an error when unavailable. */
    @GET
    @Path("/queries/{queryId}/profile")
    public void getQueryProfile(@PathParam("queryId") String queryId,
            @QueryParam("history") @DefaultValue("false") boolean history,
            @Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        if (queryProfileService == null) {
            response.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(Map.of("error", "Profile service not available"))
                    .build());
            return;
        }
        String principal = AuthenticationFilter.queryPrincipal(requestContext);
        var owner = queryManager.getStateMachine(queryId);
        if (!history && owner == null && queries != null && queries.relay(queryId, principal,
                QueryPeerRouting.Operation.PROFILE, null, requestContext, response)) {
            return;
        }
        response.resume(queryProfileService.getVisibleProfile(queryId, principal, owner, history)
                .map(profile -> Response.ok(profile).build()).orElseGet(() -> profileNotFound(queryId)));
    }

    private static Response profileNotFound(String queryId) {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(Map.of("error", "No profile for query: " + queryId))
                .build();
    }

    /** Lists summaries of all registered workers for the web UI. */
    @GET
    @Path("/workers")
    public Response listWorkers() {
        if (workerManager == null) {
            return Response.ok(List.of()).build();
        }
        List<WorkerSummary> summaries = workerManager.getAllWorkers().stream()
                .map(WebUiResource::toWorkerSummary)
                .toList();
        return Response.ok(summaries).build();
    }

    private static QuerySummary toQuerySummary(String queryId, QueryStateMachine sm) {
        String status = sm.getState().name();
        long durationMs = sm.getDurationMs();
        String user = sm.getTenantId() != null ? sm.getTenantId() : "unknown";
        Instant createdAt = sm.getCreatedAt();
        String error = sm.getError().map(e -> e.message()).orElse(null);
        return new QuerySummary(queryId, status, durationMs, user, createdAt.toString(), error);
    }

    private static WorkerSummary toWorkerSummary(WorkerInfo info) {
        return new WorkerSummary(
                info.workerId(),
                info.host(),
                info.httpPort(),
                info.healthState().name(),
                info.memoryPressure().name(),
                info.availableMemoryBytes(),
                info.runningTaskCount(),
                info.cpuUtilization(),
                info.lastHeartbeat().toString());
    }

    /**
     * Query summary for the Web UI query list.
     */
    public record QuerySummary(
            String queryId,
            String status,
            long durationMs,
            String user,
            String createdAt,
            String error) {}

    /**
     * Worker summary for the Web UI worker dashboard.
     */
    public record WorkerSummary(
            String workerId,
            String host,
            int httpPort,
            String healthState,
            String memoryPressure,
            long availableMemoryBytes,
            int runningTaskCount,
            double cpuUtilization,
            String lastHeartbeat) {}
}
