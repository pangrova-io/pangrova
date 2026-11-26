package org.pangrova.server.coordinator.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.ServerSocketChannel;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorHttpStartupCleanupTest {
    @Test
    void bindingPublishesThePortWithoutAdmittingRequestsAndStopReleasesIt() throws Exception {
        var tasks = new CoordinatorTaskTracker();
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var queries = StatusOnlyQueryManagerFixture.create(workers, tasks);
        var receiver = new TaskStatusReceiver(TaskStatusReceiverContext.withoutSpeculation(
                queries, tasks, workers, new InMemoryWriteCommitJournal()));
        var owner = new CoordinatorHttpServer(new InetSocketAddress("127.0.0.1", 0),
                CoordinatorHttpServerContext.minimal(receiver));
        int boundPort;
        try {
            owner.bind();
            boundPort = owner.getPort();
            assertThat(boundPort).isPositive();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + boundPort + "/v1/health"))
                    .timeout(Duration.ofMillis(300)).GET().build();
            assertThatThrownBy(() -> HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding()))
                    .isInstanceOf(java.net.http.HttpTimeoutException.class);
        } finally {
            owner.stop();
        }
        try (var reopened = ServerSocketChannel.open()) {
            reopened.bind(new InetSocketAddress("127.0.0.1", boundPort));
            assertThat(reopened.isOpen()).isTrue();
        }
    }

    @Test
    void startupExposesNoSplitPullRoutes() throws Exception {
        var tasks = new CoordinatorTaskTracker();
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var queries = StatusOnlyQueryManagerFixture.create(workers, tasks);
        var receiver = new TaskStatusReceiver(TaskStatusReceiverContext.withoutSpeculation(
                queries, tasks, workers, new InMemoryWriteCommitJournal()));
        var owner = new CoordinatorHttpServer(0, CoordinatorHttpServerContext.minimal(receiver));
        try {
            owner.start();
            var client = HttpClient.newHttpClient();
            for (String action : java.util.List.of("request-split", "complete-split")) {
                var request = HttpRequest.newBuilder(
                                URI.create("http://127.0.0.1:" + owner.getPort() + "/v1/task/" + action))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build();
                assertThat(client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode())
                        .as(action).isEqualTo(404);
            }
        } finally {
            owner.stop();
        }
    }

    @Test
    void occupiedPortFailureStopsAcquiredServerAndThreads() throws Exception {
        var tasks = new CoordinatorTaskTracker();
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var queries = StatusOnlyQueryManagerFixture.create(workers, tasks);
        var receiver = new TaskStatusReceiver(TaskStatusReceiverContext.withoutSpeculation(
                queries, tasks, workers, new InMemoryWriteCommitJournal()));
        try (var existing = ServerSocketChannel.open()) {
            existing.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = (InetSocketAddress) existing.getLocalAddress();
            var owner = new CoordinatorHttpServer(address, new CoordinatorHttpServerContext(
                    receiver, null, null, null, null, null, null, null));
            var field = CoordinatorHttpServer.class.getDeclaredField("server");
            field.setAccessible(true);
            var jetty = (Server) field.get(owner);
            try {
                assertThatThrownBy(owner::start).isInstanceOf(java.io.IOException.class)
                        .hasRootCauseInstanceOf(BindException.class);
                assertThat(jetty.isStopped()).as("failed startup must stop the owned server").isTrue();
                assertThat(((QueuedThreadPool) jetty.getThreadPool()).isStopped())
                        .as("failed startup must stop acquired threads").isTrue();
                assertThat(existing.isOpen()).as("the competing listener remains caller-owned").isTrue();
            } finally {
                jetty.setStopTimeout(0);
                owner.stop();
                jetty.destroy();
            }
        }
    }
}
