package org.pangrova.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.client.core.error.ErrorDetail;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * JAX-RS resource exposing query status information for monitoring.
 * Provides paginated listing of running/completed queries and per-query detail
 * including stage-level breakdown.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET /v1/query-status} — paginated list of all queries</li>
 *   <li>{@code GET /v1/query-status/{queryId}} — detailed status for a single query</li>
 *   <li>{@code GET /v1/query-status/{queryId}/stages} — stage-level breakdown</li>
 * </ul>
 */
@Path("/v1/query-status")
@Produces(MediaType.APPLICATION_JSON)
public final class QueryStatusResource {

    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final QueryManager queryManager;

    public QueryStatusResource(QueryManager queryManager) {
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
    }

    /**
     * Lists running and recently completed queries with pagination.
     *
     * @param pageToken zero-based page offset
     * @param pageSize  number of results per page (default 50, max 200)
     * @param stateFilter optional state filter (e.g. "RUNNING", "FINISHED", "FAILED")
     */
    @GET
    public Response listQueries(
            @QueryParam("pageToken") @DefaultValue("0") int pageToken,
            @QueryParam("pageSize") @DefaultValue("50") int pageSize,
            @QueryParam("state") String stateFilter) {

        int effectivePageSize = Math.max(1, Math.min(pageSize, MAX_PAGE_SIZE));
        int offset = Math.max(0, pageToken);

        Map<String, QueryStateMachine> allQueries = queryManager.getStateMachines();

        List<QueryInfo> filtered = allQueries.entrySet().stream()
                .filter(entry -> stateFilter == null || stateFilter.isBlank()
                        || entry.getValue().getState().name().equalsIgnoreCase(stateFilter))
                .sorted(Comparator.<Map.Entry<String, QueryStateMachine>, Instant>comparing(
                        entry -> entry.getValue().getCreatedAt()).reversed())
                .map(entry -> toQueryInfo(entry.getKey(), entry.getValue()))
                .toList();

        int totalCount = filtered.size();
        int fromIndex = Math.min(offset, totalCount);
        int toIndex = Math.min(fromIndex + effectivePageSize, totalCount);
        List<QueryInfo> page = filtered.subList(fromIndex, toIndex);

        String nextPageToken = toIndex < totalCount ? String.valueOf(toIndex) : null;

        var result = new QueryListResponse(page, totalCount, nextPageToken);
        return Response.ok(result).build();
    }

    /**
     * Gets detailed status for a specific query including state, timing, and error info.
     */
    @GET
    @Path("/{queryId}")
    public Response getQueryDetail(@PathParam("queryId") String queryId) {
        QueryStateMachine sm = queryManager.getStateMachine(queryId);
        if (sm == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", null,
                            "Query not found: " + queryId))
                    .build();
        }

        QueryDetail detail = toQueryDetail(queryId, sm);
        return Response.ok(detail).build();
    }

    /**
     * Gets stage-level breakdown for a specific query.
     */
    @GET
    @Path("/{queryId}/stages")
    public Response getQueryStages(@PathParam("queryId") String queryId) {
        QueryStateMachine sm = queryManager.getStateMachine(queryId);
        if (sm == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorDetail("NOT_FOUND", null,
                            "Query not found: " + queryId))
                    .build();
        }

        Map<String, StageStateMachine.State> stageStates = sm.getStageStates();
        List<StageInfo> stages = stageStates.entrySet().stream()
                .map(entry -> new StageInfo(entry.getKey(), entry.getValue().name()))
                .sorted(Comparator.comparing(StageInfo::stageId))
                .toList();

        var result = new QueryStagesResponse(queryId, sm.getState().name(), stages);
        return Response.ok(result).build();
    }

    // --- DTO construction ---

    private static QueryInfo toQueryInfo(String queryId, QueryStateMachine sm) {
        return new QueryInfo(
                queryId,
                sm.getState().name(),
                sm.getCreatedAt().toString(),
                sm.getDurationMs(),
                sm.getTenantId(),
                sm.isTerminal());
    }

    private static QueryDetail toQueryDetail(String queryId, QueryStateMachine sm) {
        String errorMessage = sm.getError().map(ErrorDetail::message).orElse(null);
        String errorCode = sm.getError().map(ErrorDetail::errorCode).orElse(null);
        int stageCount = sm.getStageStates().size();

        return new QueryDetail(
                queryId,
                sm.getState().name(),
                sm.getCreatedAt().toString(),
                sm.getDurationMs(),
                sm.getTenantId(),
                sm.isTerminal(),
                errorCode,
                errorMessage,
                stageCount);
    }

    // --- Response DTOs ---

    /** Paginated query list response. */
    public record QueryListResponse(
            List<QueryInfo> queries,
            int totalCount,
            String nextPageToken) {
        public QueryListResponse {
            queries = List.copyOf(queries);
        }
    }

    /** Summary info for a query in the list view. */
    public record QueryInfo(
            String queryId,
            String state,
            String createdAt,
            long elapsedMs,
            String tenantId,
            boolean terminal) {}

    /** Detailed info for a single query. */
    public record QueryDetail(
            String queryId,
            String state,
            String createdAt,
            long elapsedMs,
            String tenantId,
            boolean terminal,
            String errorCode,
            String errorMessage,
            int stageCount) {}

    /** Stage breakdown response. */
    public record QueryStagesResponse(
            String queryId,
            String queryState,
            List<StageInfo> stages) {
        public QueryStagesResponse {
            stages = List.copyOf(stages);
        }
    }

    /** Info for a single stage. */
    public record StageInfo(
            String stageId,
            String state) {}
}
