package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/** Actual HTTP catalog protocol inputs, not a deployed Coordinator or a metadata authority. */
final class MetadataProtocolPeer implements AutoCloseable {
    static final String NAMESPACES = "/v1/namespaces";
    static final String TABLES = "/v1/namespaces/lakehouse/tables";
    static final String DETAILS = "/v1/namespaces/lakehouse%1Fpublic/tables/orders";
    private final ExecutorService executor = ManagedExecutorFactory.newFixedThreadPool("jdbc-metadata-http", 2);
    private final ConcurrentHashMap<String, Reply> responses = new ConcurrentHashMap<>();
    private HttpServer server;

    MetadataProtocolPeer() throws Exception {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            response("/v1/config", 200, "{\"overrides\":{},\"defaults\":{}}");
            response("/v1/health", 200, "healthy");
            response(NAMESPACES, 200, "{\"namespaces\":[[\"lakehouse\"]]}");
            response(NAMESPACES + "?parent=lakehouse", 200,
                    "{\"namespaces\":[[\"lakehouse\",\"public\"],[\"lakehouse\",\"empty\"]]}");
            response(TABLES, 200, "{\"identifiers\":[{\"namespace\":[\"lakehouse\",\"public\"],\"name\":\"orders\"},"
                    + "{\"namespace\":[\"lakehouse\",\"public\"],\"name\":\"other\"}]}");
            response(DETAILS, 200, fields("bigint", "varchar(80)", "decimal(12,2)"));
            server.createContext("/v1/", exchange -> {
                try (exchange) {
                    exchange.getRequestBody().readAllBytes();
                    var reply = responses.get(exchange.getRequestURI().toASCIIString());
                    if (reply == null) reply = new Reply(404, "{\"error\":{\"message\":\"missing protocol input\",\"code\":404}}");
                    byte[] body = reply.body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(reply.status, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    void response(String path, int status, String body) { responses.put(path, new Reply(status, body)); }
    HttpServer server() { return server; }
    int port() { return server.getAddress().getPort(); }
    void start() { server.start(); }

    static String fields(String first, String second, String third) {
        return "{\"metadata\":{\"currentSchemaId\":7,\"schemas\":[{\"schemaId\":7,\"fields\":["
                + "{\"id\":91,\"name\":\"order_id\",\"type\":\"%s\",\"required\":true},"
                .formatted(first) + "{\"id\":92,\"name\":\"customer_name\",\"type\":\"%s\",\"required\":false},"
                .formatted(second) + "{\"id\":93,\"name\":\"amount\",\"type\":\"%s\",\"required\":false}]}]}}"
                .formatted(third);
    }

    @Override public void close() {
        if (server != null) server.stop(0);
        ExecutorDrain.shutdownAndAwait(executor, 5_000);
    }

    private record Reply(int status, String body) {}
}
