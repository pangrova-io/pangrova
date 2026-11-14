package org.pangrova.server.coordinator.http;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.constants.ApiVersion;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorApiVersionHttpTest {
    @Test
    void registeredVersionFilterRejectsUnsupportedApiAndDiscoveryReturnsTheRange() throws Exception {
        var tasks = new CoordinatorTaskTracker();
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var queries = StatusOnlyQueryManagerFixture.create(workers, tasks);
        var receiver = new TaskStatusReceiver(TaskStatusReceiverContext.withoutSpeculation(
                queries, tasks, workers, new InMemoryWriteCommitJournal()));
        var server = new CoordinatorHttpServer(new InetSocketAddress("127.0.0.1", 0),
                CoordinatorHttpServerContext.minimal(receiver));
        try {
            server.start();
            var base = URI.create("http://127.0.0.1:" + server.getPort());
            var client = HttpClient.newHttpClient();
            var invalid = HttpRequest.newBuilder(base.resolve("/v1/task/status-report"))
                    .header(ApiVersion.HEADER_NAME, Integer.toString(ApiVersion.CURRENT + 1))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build();
            var error = client.send(invalid, HttpResponse.BodyHandlers.ofString());
            assertThat(error.statusCode()).isEqualTo(422);
            assertThat(error.headers().allValues(ApiVersion.HEADER_NAME)).containsExactly(Integer.toString(ApiVersion.CURRENT));
            var mapper = JsonMapperFactory.create();
            assertThat(mapper.readTree(error.body()).path("error_code").textValue()).isEqualTo(ApiVersion.ERROR_CODE_UNSUPPORTED);

            var discover = HttpRequest.newBuilder(base.resolve("/v1/server/info"))
                    .header(ApiVersion.HEADER_NAME, "future").GET().build();
            var info = client.send(discover, HttpResponse.BodyHandlers.ofString());
            assertThat(info.statusCode()).isEqualTo(200);
            assertThat(info.headers().allValues(ApiVersion.HEADER_NAME)).containsExactly(Integer.toString(ApiVersion.CURRENT));
            var body = mapper.readTree(info.body());
            assertThat(body.get("api_version").intValue()).isEqualTo(ApiVersion.CURRENT);
            assertThat(body.get("min_api_version").intValue()).isEqualTo(ApiVersion.MIN_SUPPORTED);
        } finally {
            server.stop();
        }
    }
}
