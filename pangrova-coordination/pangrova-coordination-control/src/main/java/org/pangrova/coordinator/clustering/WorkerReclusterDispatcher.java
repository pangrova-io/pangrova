package org.pangrova.coordinator.clustering;

import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.connector.api.clustering.ClusteringMaintenance;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.protocol.rest.maintenance.ReclusterStageRequest;
import org.pangrova.protocol.rest.maintenance.ReclusterStageResult;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/** Dispatches staging to live Workers while the Coordinator retains publication authority. */
public final class WorkerReclusterDispatcher {
    private static final int MAX_ATTEMPTS = 3;
    private static final int HTTP_OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int BAD_GATEWAY = 502;
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int GATEWAY_TIMEOUT = 504;
    private static final Set<Integer> UNAVAILABLE = Set.of(
            NOT_FOUND, TOO_MANY_REQUESTS, BAD_GATEWAY, SERVICE_UNAVAILABLE, GATEWAY_TIMEOUT);
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Supplier<List<WorkerInfo>> workers;
    private final String secret;

    /** Uses the process Worker inventory and shared internal authentication. */
    public WorkerReclusterDispatcher(Supplier<List<WorkerInfo>> workers, String secret) {
        this.workers = java.util.Objects.requireNonNull(workers, "workers");
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("Worker RECLUSTER requires internal authentication");
        }
        this.secret = secret;
    }

    /** Binds the dispatch lease to one catalog, tenant, principal, table and durable run. */
    public ClusteringMaintenance.StageDispatcher bind(CatalogDescriptor catalog, QualifiedTableName table,
            String tenant, String principal, UUID runId) {
        String binding = Base64.getEncoder().encodeToString(CatalogExecutionBindings.from(catalog).toByteArray());
        return (revision, fence) -> stage(binding, table, tenant, principal, runId, revision, fence);
    }

    private ClusteringMaintenance.StagedRewrite stage(String binding, QualifiedTableName table,
            String tenant, String principal, UUID runId, String revision, Runnable fence) {
        var exhausted = new IllegalStateException("No available Worker could stage RECLUSTER");
        var candidates = workers.get().stream().sorted(Comparator.comparingInt(WorkerInfo::inFlightTaskCount)
                .thenComparing(WorkerInfo::workerId)).limit(MAX_ATTEMPTS).toList();
        for (var worker : candidates) {
            fence.run();
            var work = new ReclusterStageRequest(binding, tenant, principal, table.schema(), table.table(),
                    revision, runId, UUID.randomUUID(), System.currentTimeMillis() + ReclusterStageRequest.TIMEOUT_MS);
            try {
                var result = stageOn(worker, work);
                if (result != null) {
                    fence.run();
                    return new ClusteringMaintenance.StagedRewrite(result.attemptId(), result.files());
                }
                exhausted.addSuppressed(new IllegalStateException(
                        "Worker could not admit staging: " + worker.workerId()));
            } catch (IOException unavailable) {
                exhausted.addSuppressed(unavailable);
            } catch (InterruptedException cancelled) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("RECLUSTER staging was interrupted", cancelled);
            }
        }
        throw exhausted;
    }

    private ReclusterStageResult stageOn(WorkerInfo worker, ReclusterStageRequest work)
            throws IOException, InterruptedException {
        var mapper = JsonMapperFactory.shared();
        var request = HttpRequest.newBuilder(endpoint(worker)).header("X-Internal-Auth", secret)
                .header("Content-Type", "application/json")
                .timeout(Duration.ofMillis(ReclusterStageRequest.TIMEOUT_MS))
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(work))).build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var body = response.body()) {
            int status = response.statusCode();
            if (UNAVAILABLE.contains(status)) return null;
            if (status != HTTP_OK) {
                throw new IllegalStateException("Worker rejected RECLUSTER staging: HTTP " + status);
            }
            byte[] bytes = body.readNBytes(ReclusterStageRequest.MAX_RESULT_BYTES + 1);
            if (bytes.length > ReclusterStageRequest.MAX_RESULT_BYTES) {
                throw new IllegalStateException("RECLUSTER staging response exceeds its admitted size");
            }
            ReclusterStageResult result;
            try {
                result = mapper.readValue(bytes, ReclusterStageResult.class);
            } catch (IOException invalid) {
                throw new IllegalStateException("Worker returned malformed RECLUSTER staging evidence", invalid);
            }
            if (!work.attemptId().equals(result.attemptId())) {
                throw new IllegalStateException("Worker returned evidence for a different RECLUSTER attempt");
            }
            return result;
        }
    }

    private static URI endpoint(WorkerInfo worker) {
        try {
            return new URI("http", null, worker.host(), worker.httpPort(), ReclusterStageRequest.PATH, null, null);
        } catch (URISyntaxException invalid) {
            throw new IllegalArgumentException("Invalid Worker endpoint for RECLUSTER", invalid);
        }
    }
}
