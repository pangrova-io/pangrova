package org.pangrova.server.coordinator.http;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.runtime.QueryManager;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorQueryStatusWaitHttpTest {
    @Test
    void sameObserverHasOneWaitAndWakesForEndpointWithoutAStateTransition() throws Exception {
        try (var peer = new Peer()) {
            var state = peer.running("q-owner");
            QueryStatusResponse initial = status(peer.get("/v1/query/q-owner/state"));
            assertThat(initial.state()).isEqualTo("RUNNING");
            assertThat(initial.drained()).isFalse();
            String observer = UUID.randomUUID().toString();
            var first = peer.waitFor("q-owner", initial.version(), 5_000, observer);
            var second = peer.waitFor("q-owner", initial.version(), 5_000, observer);
            var duplicate = (HttpResponse<?>) CompletableFuture.anyOf(first, second).get(4, TimeUnit.SECONDS);
            assertThat(duplicate.statusCode()).isEqualTo(409);
            var endpoint = new ResultEndpoint("worker", 1234, "issued", "grpc");
            state.setResultEndpoint(endpoint);
            var success = first.isDone() && first.join().statusCode() == 409 ? second : first;
            QueryStatusResponse changed = status(success.get(4, TimeUnit.SECONDS));
            assertThat(changed.version()).isEqualTo(initial.version() + 1);
            assertThat(changed.state()).isEqualTo("RUNNING");
            assertThat(changed.resultEndpoint()).isEqualTo(endpoint);
        }
    }

    @Test
    void timeoutUnregistersObserverAndPeerJournalIsReservedForDiagnosticReads() throws Exception {
        try (var peer = new Peer()) {
            peer.running("q-timeout");
            QueryStatusResponse initial = status(peer.get("/v1/query/q-timeout/state"));
            String observer = UUID.randomUUID().toString();
            for (int i = 0; i < 2; i++) {
                QueryStatusResponse expired = status(peer.waitFor("q-timeout", initial.version(), 40, observer)
                        .get(4, TimeUnit.SECONDS));
                assertThat(expired).isEqualTo(initial);
            }
            assertThat(peer.metrics.active.get()).isZero();
            assertThat(peer.metrics.outcomes).containsExactlyInAnyOrderEntriesOf(Map.of(
                    Map.of("target", "NEXT_VERSION", "outcome", "TIMEOUT"), 2L));
            var session = new org.pangrova.query.api.session.JournalSessionPayload("q-peer", "", "anonymous");
            String payload = org.pangrova.common.json.JsonMapperFactory.writer().writeValueAsString(session);
            peer.queries.getStateJournal().appendQueryEventSync(new QueryStateEvent(
                    JournalEventContext.create(System.currentTimeMillis(), "unreachable-peer"),
                    "q-peer", null, payload, "QUEUED"));
            var context = JournalEventContext.create(System.currentTimeMillis(), "unreachable-peer");
            peer.queries.getStateJournal().appendQueryEventSync(
                    new QueryStateEvent(context, "q-peer", null, null, "RUNNING"));
            var ownerMiss = peer.waitFor("q-peer", 0, 40, observer).get(4, TimeUnit.SECONDS);
            assertThat(ownerMiss.statusCode()).isEqualTo(404);
            assertThat(ownerMiss.body()).contains("QUERY_OWNER_UNAVAILABLE");
            assertThat(peer.get("/v1/query/q-peer/state").statusCode()).isEqualTo(404);
            assertThat(peer.get("/v1/query/q-peer/state?history=true&afterVersion=0").statusCode())
                    .isEqualTo(400);
            HttpResponse<String> historicalResponse = peer.get("/v1/query/q-peer/state?history=true");
            assertThat(historicalResponse.statusCode()).isEqualTo(200);
            QueryStatusResponse history = JsonMapperFactory.shared().readValue(
                    historicalResponse.body(), QueryStatusResponse.class);
            assertThat(history.queryId()).isEqualTo("q-peer");
            assertThat(history.state()).isEqualTo("RUNNING");
            JsonNode historicalJson = JsonMapperFactory.shared().readTree(historicalResponse.body());
            assertThat(historicalJson.has("result_disposition")).isFalse();
            assertThat(historicalJson.has("result_endpoint")).isFalse();
            assertThat(historicalJson.has("version")).isFalse();
            assertThat(historicalJson.has("drained")).isFalse();
        }
    }

    @Test
    void targetedObservationWaitsAcrossFinishingAndSeparatesPhysicalRelease() throws Exception {
        try (var peer = new Peer()) {
            var state = peer.running("q-events");
            String observer = UUID.randomUUID().toString();
            var terminal = peer.waitFor("q-events", 0, 5_000, observer, QueryWaitTarget.TERMINAL);
            var duplicate = peer.waitFor("q-events", 0, 5_000, observer, QueryWaitTarget.TERMINAL);
            var refusal = (HttpResponse<?>) CompletableFuture.anyOf(terminal, duplicate).get(4, TimeUnit.SECONDS);
            assertThat(refusal.statusCode()).isEqualTo(409);
            var accepted = terminal.isDone() && terminal.join().statusCode() == 409 ? duplicate : terminal;

            assertThat(state.tryTransition(QueryState.FINISHING)).isTrue();
            assertThat(state.tryTransition(QueryState.FINISHED)).isTrue();
            var finished = status(accepted.get(4, TimeUnit.SECONDS));
            assertThat(finished.state()).isEqualTo("FINISHED");
            assertThat(finished.drained()).isFalse();

            String drainObserver = UUID.randomUUID().toString();
            var released = peer.waitFor("q-events", 0, 5_000, drainObserver, QueryWaitTarget.DRAINED);
            var repeated = peer.waitFor("q-events", 0, 5_000, drainObserver, QueryWaitTarget.DRAINED);
            var duplicateDrain = (HttpResponse<?>) CompletableFuture.anyOf(released, repeated).get(4, TimeUnit.SECONDS);
            assertThat(duplicateDrain.statusCode()).isEqualTo(409);
            var acceptedDrain = released.isDone() && released.join().statusCode() == 409 ? repeated : released;
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(peer.metrics.active.get()).isOne());
            assertThat(state.claimPhysicalCleanup(true)).isTrue();
            var drained = status(acceptedDrain.get(4, TimeUnit.SECONDS));
            assertThat(drained.drained()).isTrue();
            assertThat(drained.version()).isGreaterThan(finished.version());
            assertThat(peer.metrics.active.get()).isZero();
            assertThat(peer.metrics.outcomes).containsExactlyInAnyOrderEntriesOf(Map.of(
                    Map.of("target", "TERMINAL", "outcome", "CHANGED"), 1L,
                    Map.of("target", "TERMINAL", "outcome", "DUPLICATE"), 1L,
                    Map.of("target", "DRAINED", "outcome", "CHANGED"), 1L,
                    Map.of("target", "DRAINED", "outcome", "DUPLICATE"), 1L));
        }
    }

    static QueryStatusResponse status(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).as("status body %s", response.body()).isEqualTo(200);
        return JsonMapperFactory.shared().readValue(response.body(), QueryStatusResponse.class);
    }

    static final class Peer implements AutoCloseable {
        private final StatusWaitMetrics metrics = new StatusWaitMetrics();
        private final QueryManager queries;
        private final CoordinatorHttpServer server;
        private final HttpClient client = HttpClient.newHttpClient();

        Peer() throws Exception {
            this(new InMemoryQueryStateJournal());
        }

        Peer(QueryStateJournal journal) throws Exception {
            var tasks = new CoordinatorTaskTracker();
            var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            queries = StatusOnlyQueryManagerFixture.create(workers, tasks, journal);
            var receiver = new TaskStatusReceiver(TaskStatusReceiverContext.withoutSpeculation(
                    queries, tasks, workers, new InMemoryWriteCommitJournal()));
            server = new CoordinatorHttpServer(new InetSocketAddress("127.0.0.1", 0),
                    new CoordinatorHttpServerContext(receiver, queries, null, null, null, null, null, null,
                            null, null, null, null, null, null, metrics));
            server.start();
        }

        QueryStateMachine running(String queryId) {
            var state = new QueryStateMachine();
            assertThat(state.tryTransition(QueryState.PLANNING)).isTrue();
            assertThat(state.tryTransition(QueryState.SCHEDULING)).isTrue();
            assertThat(state.tryTransition(QueryState.RUNNING)).isTrue();
            queries.registerStateMachine(queryId, state);
            return state;
        }

        HttpResponse<String> get(String path) throws Exception {
            return client.send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
        }

        CompletableFuture<HttpResponse<String>> waitFor(String queryId, long version, long duration, String observer) {
            return waitFor(queryId, version, duration, observer, QueryWaitTarget.NEXT_VERSION);
        }

        CompletableFuture<HttpResponse<String>> waitFor(String queryId, long version, long duration, String observer,
                QueryWaitTarget target) {
            return client.sendAsync(request("/v1/query/" + queryId + "/state?afterVersion="
                    + version + "&waitMs=" + duration + "&until=" + target.name())
                            .header("X-Pangrova-Query-Observer", observer).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
        }

        private HttpRequest.Builder request(String path) {
            return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + path))
                    .timeout(Duration.ofSeconds(8));
        }

        @Override
        public void close() throws Exception {
            server.stop();
            queries.getStateJournal().close();
        }
    }

    private static final class StatusWaitMetrics extends NoOpMetricsProvider {
        private final AtomicLong active = new AtomicLong();
        private final Map<Map<String, String>, Long> outcomes = new ConcurrentHashMap<>();

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return "pangrova.query.status_wait.active".equals(name)
                    ? active::addAndGet : super.createUpDownCounter(name, description, unit);
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            if (!"pangrova.query.status_wait.completed".equals(name)) {
                return super.createCounter(name, description, unit);
            }
            return new LongCounter() {
                @Override
                public void add(long value) {
                    throw new AssertionError("Status wait completion requires target and outcome");
                }

                @Override
                public void add(long value, MetricAttributes attributes) {
                    outcomes.merge(attributes.asMap(), value, Long::sum);
                }
            };
        }
    }
}
