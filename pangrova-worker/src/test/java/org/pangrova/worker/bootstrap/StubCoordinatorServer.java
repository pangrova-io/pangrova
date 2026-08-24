package org.pangrova.worker.bootstrap;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Lightweight JDK HttpServer that simulates Coordinator registration
 * and task status reporting endpoints for Worker bootstrap tests.
 *
 * <p>Responds 200 OK to POST /v1/workers/register and
 * POST /v1/task-status/report, which is all WorkerBootstrap needs
 * to complete its startup sequence without a real Coordinator.</p>
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
        server.createContext("/v1/workers/register", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            byte[] response = "{\"status\":\"ok\"}".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        server.createContext("/v1/task-status/report", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            byte[] response = "{\"status\":\"ok\"}".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        server.setExecutor(null);
        server.start();
        return new StubCoordinatorServer(server);
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
