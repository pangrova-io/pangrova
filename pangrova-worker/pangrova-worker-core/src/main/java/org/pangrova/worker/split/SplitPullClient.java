package org.pangrova.worker.split;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.task.SplitRequest;
import org.pangrova.protocol.rest.task.SplitResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Worker-side client for pull-mode split assignment.
 * Requests splits on demand from the Coordinator and notifies completion.
 *
 * <p>Activated when {@code pangrova.split.assignment.pull-mode.enabled=true}.
 * Worker bootstrap creates this client and injects it into the task lifecycle.
 * When pull-mode is disabled (default), splits are embedded in task requests
 * via push mode and this client is not instantiated.</p>
 */
@ThreadSafe
public final class SplitPullClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SplitPullClient.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    /** HTTP status indicating the request succeeded. */
    private static final int HTTP_OK = 200;

    /** HTTP status indicating pull mode is disabled on the coordinator. */
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String coordinatorBaseUrl;
    private final String workerId;
    private final List<String> workerLocality;

    public SplitPullClient(String coordinatorBaseUrl,
                           String workerId, List<String> workerLocality) {
        this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                coordinatorBaseUrl, workerId, workerLocality);
    }

    SplitPullClient(HttpClient httpClient, String coordinatorBaseUrl,
                    String workerId,
                    List<String> workerLocality) {
        this.httpClient = checkNotNull(httpClient, "httpClient must not be null");
        this.coordinatorBaseUrl = checkNotNull(coordinatorBaseUrl, "coordinatorBaseUrl must not be null");
        this.workerId = checkNotNull(workerId, "workerId must not be null");
        this.workerLocality = workerLocality != null ? List.copyOf(workerLocality) : List.of();
        this.mapper = new ObjectMapper();
    }

    /**
     * Requests the next available split for the given query and stage.
     *
     * @param queryId the query identifier
     * @param stageId the stage identifier
     * @return the split response, or empty if the coordinator is unreachable
     */
    public Optional<SplitResponse> requestSplit(String queryId, int stageId) {
        SplitRequest request = new SplitRequest(queryId, stageId, workerId, workerLocality);
        String url = coordinatorBaseUrl + "/v1/task/request-split";
        try {
            HttpResponse<String> response = sendSplitRequest(url, request);
            return parseSplitResponse(response, queryId, stageId);
        } catch (IOException e) {
            LOG.warn("event=split_request_io_error queryId={} stageId={} error={}",
                    queryId, stageId, e.getMessage());
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=split_request_interrupted queryId={} stageId={}", queryId, stageId);
            return Optional.empty();
        }
    }

    private HttpResponse<String> sendSplitRequest(String url, SplitRequest request)
            throws IOException, InterruptedException {
        byte[] body = mapper.writeValueAsBytes(request);
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private Optional<SplitResponse> parseSplitResponse(HttpResponse<String> response,
                                                       String queryId, int stageId)
            throws IOException {
        if (response.statusCode() == HTTP_SERVICE_UNAVAILABLE) {
            LOG.debug("event=pull_mode_disabled queryId={} stageId={}", queryId, stageId);
            return Optional.empty();
        }
        if (response.statusCode() != HTTP_OK) {
            LOG.warn("event=split_request_failed queryId={} stageId={} status={}",
                    queryId, stageId, response.statusCode());
            return Optional.empty();
        }
        SplitResponse splitResponse = mapper.readValue(response.body(), SplitResponse.class);
        LOG.debug("event=split_received queryId={} stageId={} hasSplit={} splitIndex={}",
                queryId, stageId, splitResponse.hasSplit(), splitResponse.splitIndex());
        return Optional.of(splitResponse);
    }

    /**
     * Notifies the Coordinator that a split has been processed.
     *
     * @param queryId    the query identifier
     * @param stageId    the stage identifier
     * @param splitIndex the index of the completed split
     */
    public void completeSplit(String queryId, int stageId, int splitIndex) {
        String url = coordinatorBaseUrl + "/v1/task/complete-split";
        try {
            HttpResponse<String> response = sendCompletionRequest(url, queryId, stageId, splitIndex);
            if (response.statusCode() != HTTP_OK) {
                LOG.warn("event=split_complete_failed queryId={} stageId={} splitIndex={} status={}",
                        queryId, stageId, splitIndex, response.statusCode());
            }
        } catch (IOException e) {
            LOG.warn("event=split_complete_io_error queryId={} stageId={} splitIndex={} error={}",
                    queryId, stageId, splitIndex, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private HttpResponse<String> sendCompletionRequest(String url, String queryId,
                                                       int stageId, int splitIndex)
            throws IOException, InterruptedException {
        String json = mapper.writeValueAsString(
                new SplitCompletion(queryId, stageId, splitIndex));
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    @Override
    public void close() {
        LOG.debug("event=split_pull_client_closed workerId={}", workerId);
    }

    record SplitCompletion(String queryId, int stageId, int splitIndex) {}
}
