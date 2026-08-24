/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.ai.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Fetches serialized vector search results from a worker's HTTP endpoint.
 *
 * <p>Calls {@code GET /v1/task/{taskId}/vector-result} on the target worker and
 * returns the raw proto bytes. A 404 response is treated as an empty result
 * (the worker completed with no vector matches). Any other non-200 response or
 * network error is logged and returned as empty so that the coordinator can
 * continue merging results from other workers.</p>
 *
 * <p>Thread-safe: {@link HttpClient} is thread-safe and shared across calls.</p>
 */
public final class WorkerVectorResultFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerVectorResultFetcher.class);

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int HTTP_OK = 200;
    private static final int HTTP_NOT_FOUND = 404;

    private final HttpClient httpClient;
    private final Duration requestTimeout;

    /** Creates a fetcher with a shared HTTP client and default timeouts. */
    public WorkerVectorResultFetcher() {
        this(HttpClient.newBuilder()
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .build(),
                DEFAULT_REQUEST_TIMEOUT);
    }

    WorkerVectorResultFetcher(HttpClient httpClient, Duration requestTimeout) {
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Fetches the vector search result bytes for the given task from a worker.
     *
     * @param workerHost worker hostname
     * @param workerHttpPort worker HTTP port
     * @param taskId coordinator task identifier
     * @return raw proto bytes if the worker has a result, empty otherwise
     */
    public Optional<byte[]> fetch(String workerHost, int workerHttpPort, String taskId) {
        String url = "http://" + workerHost + ":" + workerHttpPort
                + "/v1/task/" + taskId + "/vector-result";
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(requestTimeout)
                    .GET()
                    .build();
            HttpResponse<byte[]> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == HTTP_OK) {
                byte[] body = response.body();
                LOG.debug("event=vector_result_fetched_from_worker host={} taskId={} bytes={}",
                        workerHost, taskId, body.length);
                return Optional.of(body);
            }
            if (response.statusCode() == HTTP_NOT_FOUND) {
                LOG.debug("event=vector_result_not_found_on_worker host={} taskId={}",
                        workerHost, taskId);
                return Optional.empty();
            }
            LOG.warn("event=vector_result_fetch_unexpected_status host={} taskId={} status={}",
                    workerHost, taskId, response.statusCode());
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=vector_result_fetch_interrupted host={} taskId={}", workerHost, taskId);
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("event=vector_result_fetch_failed host={} taskId={} error={}",
                    workerHost, taskId, e.getMessage());
            return Optional.empty();
        }
    }
}
