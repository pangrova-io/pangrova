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
package org.pangrova.coordinator.indexing.ai.lifecycle;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.indexing.api.publication.IndexAvailabilityPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Default implementation of {@link IndexAvailabilityPublisher} that notifies workers
 * via HTTP POST to their load-index endpoint.
 *
 * <p>When a DiskANN index build completes, this publisher sends a notification to all
 * target workers (or all active workers if no targets specified) containing the index
 * storage path. Workers receiving this notification load the index into their native
 * engine and begin reporting the shard via heartbeat.</p>
 */
public final class DefaultIndexAvailabilityPublisher implements IndexAvailabilityPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultIndexAvailabilityPublisher.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int HTTP_STATUS_OK = 200;
    private static final int HTTP_STATUS_MULTIPLE_CHOICES = 300;

    private final WorkerManager workerManager;
    private final HttpClient httpClient;

    /** Creates a publisher broadcasting index-ready events to workers over HTTP. */
    public DefaultIndexAvailabilityPublisher(WorkerManager workerManager) {
        this.workerManager = workerManager;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public void publishIndexReady(IndexReadyEvent event) {
        List<WorkerInfo> targets = resolveTargets(event);
        if (targets.isEmpty()) {
            LOG.warn("event=index_publish_no_targets indexId={} table={}",
                    event.indexId(), event.tableName());
            return;
        }

        String payload = buildPayload(event);
        int successCount = 0;
        int failureCount = 0;

        for (WorkerInfo worker : targets) {
            try {
                String url = String.format("http://%s:%d/v1/index/load",
                        worker.host(), worker.httpPort());
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(REQUEST_TIMEOUT)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();
                HttpResponse<String> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= HTTP_STATUS_OK
                        && response.statusCode() < HTTP_STATUS_MULTIPLE_CHOICES) {
                    successCount++;
                } else {
                    failureCount++;
                    LOG.warn("event=index_publish_worker_rejected workerId={} status={} body={}",
                            worker.workerId(), response.statusCode(), response.body());
                }
            } catch (Exception e) {
                failureCount++;
                LOG.warn("event=index_publish_worker_failed workerId={} error={}",
                        worker.workerId(), e.getMessage());
            }
        }

        LOG.info("event=index_publish_complete indexId={} table={} algorithm={} "
                        + "targetWorkers={} success={} failures={}",
                event.indexId(), event.tableName(), event.algorithmName(),
                targets.size(), successCount, failureCount);
    }

    private List<WorkerInfo> resolveTargets(IndexReadyEvent event) {
        if (event.targetWorkerIds().isEmpty()) {
            return workerManager.getActiveWorkers();
        }
        List<WorkerInfo> allWorkers = workerManager.getActiveWorkers();
        return allWorkers.stream()
                .filter(w -> event.targetWorkerIds().contains(w.workerId()))
                .toList();
    }

    private static String buildPayload(IndexReadyEvent event) {
        return String.format(
                "{\"indexId\":\"%s\",\"tableName\":\"%s\",\"vectorColumn\":\"%s\","
                        + "\"storagePath\":\"%s\",\"generation\":%d,\"rowCount\":%d,"
                        + "\"algorithmName\":\"%s\"}",
                event.indexId(), event.tableName(),
                event.vectorColumn() != null ? event.vectorColumn() : "",
                event.storagePath(), event.generation(), event.rowCount(),
                event.algorithmName());
    }
}
