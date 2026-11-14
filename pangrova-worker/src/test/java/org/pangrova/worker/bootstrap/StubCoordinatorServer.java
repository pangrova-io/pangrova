package org.pangrova.worker.bootstrap;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight JDK HttpServer that simulates Coordinator registration
 * and task status reporting endpoints for Worker bootstrap tests.
 *
 * <p>Provides registration, heartbeat, reporting and deregistration for
 * component lifecycle tests, not distributed execution evidence.</p>
 */
public final class StubCoordinatorServer implements AutoCloseable {

    private final HttpServer server;

    private StubCoordinatorServer(HttpServer server) {
        this.server = server;
    }

    /**
     * Starts a stub coordinator on an ephemeral port.
     *
     * @return a started server ready to accept registration requests
     */
    public static StubCoordinatorServer start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/register", exchange -> respond(exchange, "registered"));
        server.createContext("/v1/worker/heartbeat", exchange -> respond(exchange, "ok"));
        server.createContext("/v1/task-status/report", exchange -> respond(exchange, "ok"));
        server.createContext("/v1/workers/", exchange -> {
            if (!exchange.getRequestMethod().equals("DELETE")
                    || exchange.getRequestURI().getPath().equals("/v1/workers/")) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            respond(exchange, "deregistered");
        });
        server.setExecutor(null);
        server.start();
        return new StubCoordinatorServer(server);
    }

    private static void respond(HttpExchange exchange, String status) throws IOException {
        exchange.getRequestBody().readAllBytes();
        byte[] response = ("{\"status\":\"" + status + "\",\"coordinator_epoch\":1}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
    }

    /** Returns the base URL (e.g. "http://127.0.0.1:54321"). */
    public String baseUrl() {
        InetSocketAddress addr = server.getAddress();
        return "http://127.0.0.1:" + addr.getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
