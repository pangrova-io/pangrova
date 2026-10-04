package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.client.rest.RetryPolicy;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.protocol.rest.query.ResultEndpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Real HTTP protocol inputs and owned JDBC consumer threads; not a Coordinator or SQL engine. */
@NotThreadSafe
final class StatementCancellationPeer implements AutoCloseable {
    private final ExecutorService http = ManagedExecutorFactory.newFixedThreadPool("jdbc-cancel-http", 4);
    private final ExecutorService consumers = ManagedExecutorFactory.newFixedThreadPool("jdbc-cancel-consumer", 2);
    private final ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Query> submissions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PangrovaPreparedStatement> preparedStatements = new ConcurrentLinkedQueue<>();
    private final AtomicInteger submits = new AtomicInteger();
    private final AtomicInteger requests = new AtomicInteger();
    private HttpServer server;
    private PangrovaConnection connection;
    private PangrovaStatement statement;

    StatementCancellationPeer() throws Exception {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(http);
            server.createContext("/v1/query/", this::serve);
            server.start();
            var client = PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(server.getAddress().getPort()).connectTimeout(Duration.ofSeconds(2))
                    .requestTimeout(Duration.ofSeconds(2)).retryPolicy(RetryPolicy.NONE).build();
            connection = new PangrovaConnection(client, null);
            statement = new PangrovaStatement(connection);
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    Query query(String id, ResultEndpoint endpoint) {
        var query = new Query(id, endpoint);
        if (queries.putIfAbsent(id, query) != null) throw new IllegalArgumentException("Duplicate query identity");
        submissions.add(query);
        return query;
    }

    PangrovaStatement statement() { return statement; }
    int submissions() { return submits.get(); }
    int requests() { return requests.get(); }

    PangrovaPreparedStatement prepare(String sql) throws SQLException {
        var prepared = (PangrovaPreparedStatement) connection.prepareStatement(sql);
        preparedStatements.add(prepared);
        return prepared;
    }

    Future<SQLException> executePreparedAsync(PangrovaPreparedStatement prepared) {
        return consumers.submit(() -> {
            try (var result = prepared.executeQuery()) { return null; }
            catch (SQLException failure) { return failure; }
        });
    }

    Future<SQLException> executeAsync() {
        return consumers.submit(() -> {
            try (var result = statement.executeQuery("SELECT 42")) { return null; }
            catch (SQLException failure) { return failure; }
        });
    }

    Future<Void> cancelAsync() {
        return consumers.submit(() -> { statement.cancel(); return null; });
    }

    private void serve(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            requests.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/v1/query/submit")) {
                submits.incrementAndGet();
                var query = submissions.poll();
                if (query == null) throw new IllegalStateException("No protocol submission configured");
                reply(exchange, new Reply(200, query.metadata()));
                return;
            }
            List<String> components = List.of(path.split("/"));
            var query = queries.get(components.get(3));
            if (query == null) throw new IllegalStateException("Unexpected query identity");
            if (path.endsWith("/cancel")) serveCancel(exchange, query);
            else {
                query.polled.countDown();
                Reply statusFailure = query.statusFailure.get();
                reply(exchange, statusFailure == null ? new Reply(200, query.metadata()) : statusFailure);
            }
        }
    }

    private void serveCancel(HttpExchange exchange, Query query) throws IOException {
        query.cancelRequests.incrementAndGet();
        query.lastCancelled.set(exchange.getRequestURI().getPath().split("/")[3]);
        var block = query.cancelBlock.getAndSet(null);
        if (block != null) block.await();
        Reply response = query.replies.poll();
        if (response == null) response = accepted(query.id);
        if (response.status == 202 && response.body.equals(accepted(query.id).body)) query.state.set("CANCELLED");
        reply(exchange, response);
    }

    static Reply accepted(String id) { return new Reply(202, cancelBody(id, true, "CANCEL_ACCEPTED")); }
    static Reply absent(String id) { return new Reply(404, cancelBody(id, false, "NOT_FOUND")); }
    private static String cancelBody(String id, boolean cancelled, String outcome) {
        return "{\"query_id\":\"%s\",\"cancelled\":%s,\"outcome\":\"%s\"}".formatted(id, cancelled, outcome);
    }
    private static void reply(HttpExchange exchange, Reply response) throws IOException {
        byte[] bytes = response.body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @Override public void close() throws Exception {
        queries.values().forEach(query -> {
            query.state.set("CANCELLED");
            query.blocks.forEach(ResponseBlock::close);
        });
        // A completed/cancelled Future alone is not evidence that its callback exited.
        ExecutorDrain.shutdownAndAwait(consumers, 5_000);
        Exception failure = null;
        for (var prepared : preparedStatements) {
            try { prepared.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
        }
        try { if (statement != null) statement.close(); } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        try { if (connection != null) connection.close(); } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        if (server != null) server.stop(0);
        try { ExecutorDrain.shutdownAndAwait(http, 5_000); } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        if (failure != null) throw failure;
    }

    record Reply(int status, String body) {}

    static final class Query {
        final String id;
        final ResultEndpoint endpoint;
        final AtomicReference<String> state;
        final CountDownLatch polled = new CountDownLatch(1);
        final AtomicInteger cancelRequests = new AtomicInteger();
        final AtomicReference<String> lastCancelled = new AtomicReference<>();
        final ConcurrentLinkedQueue<Reply> replies = new ConcurrentLinkedQueue<>();
        final AtomicReference<Reply> statusFailure = new AtomicReference<>();
        final AtomicReference<ResponseBlock> cancelBlock = new AtomicReference<>();
        final ConcurrentLinkedQueue<ResponseBlock> blocks = new ConcurrentLinkedQueue<>();

        private Query(String id, ResultEndpoint endpoint) {
            this.id = id;
            this.endpoint = endpoint;
            state = new AtomicReference<>(endpoint == null ? "RUNNING" : "FINISHED");
        }
        ResponseBlock blockCancel() {
            var block = new ResponseBlock();
            if (!cancelBlock.compareAndSet(null, block)) throw new IllegalStateException("Cancel already held");
            blocks.add(block);
            return block;
        }
        private String metadata() {
            String descriptor = endpoint == null ? "" : ",\"result_endpoint\":{\"host\":\"%s\",\"port\":%d,"
                    .formatted(endpoint.host(), endpoint.port()) + "\"ticket\":\"%s\",\"protocol\":\"%s\"}"
                    .formatted(endpoint.ticket(), endpoint.protocol());
            return "{\"query_id\":\"%s\",\"state\":\"%s\",\"column_names\":[\"value\"]%s}"
                    .formatted(id, state.get(), descriptor);
        }
    }

    static final class ResponseBlock implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private void await() {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("HTTP response barrier timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("HTTP response barrier interrupted", interrupted);
            }
        }
        @Override public void close() { release.countDown(); }
    }
}
