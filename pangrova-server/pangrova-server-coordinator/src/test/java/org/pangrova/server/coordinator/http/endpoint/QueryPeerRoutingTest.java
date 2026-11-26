package org.pangrova.server.coordinator.http.endpoint;

import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.enums.QueryWaitTarget;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class QueryPeerRoutingTest {

    @Test
    void refusesModifiedResponseBytesEvenWithAnAuthenticOwnerSignature() throws Exception {
        var upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var owner = new QueryOwnerEndpoint("owner", "current", "127.0.0.1", upstream.getAddress().getPort());
        var ingress = new QueryOwnerEndpoint("ingress", "relay", "127.0.0.1", 1);
        try (var routing = new QueryPeerRouting("distinct-test-query-peer-signing-key")) {
            var observed = new CompletableFuture<QueryPeerRouting.Command>();
            upstream.createContext("/" + QueryPeerRouting.PATH, request -> {
                try (request) {
                    String signature = request.getRequestHeaders().getFirst(QueryPeerRouting.SIGNATURE);
                    byte[] command = request.getRequestBody().readAllBytes();
                    observed.complete(routing.authenticate(command, signature, owner));
                    byte[] authentic = "{\"version\":1}".getBytes(StandardCharsets.UTF_8);
                    byte[] modified = "{\"version\":2}".getBytes(StandardCharsets.UTF_8);
                    request.getResponseHeaders().set(QueryPeerRouting.SIGNATURE,
                            routing.responseSignature(signature, 200, authentic));
                    request.sendResponseHeaders(200, modified.length);
                    request.getResponseBody().write(modified);
                }
            });
            upstream.start();
            String queryId = "query-original";
            String principal = "alice";
            var ticket = routing.issue(owner, queryId, principal).routeTicket();
            var reply = new CompletableFuture<Response>();
            var observation = new QueryPeerRouting.Observation(0, 0, QueryWaitTarget.NEXT_VERSION, null);
            assertThat(routing.forward(ticket, queryId, principal, ingress,
                    QueryPeerRouting.Operation.STATE, observation, capture(reply))).isTrue();
            assertThat(reply.get(5, TimeUnit.SECONDS).getStatus()).isEqualTo(503);
            assertThat(observed.get(5, TimeUnit.SECONDS)).satisfies(command -> {
                assertThat(command.queryId()).isEqualTo(queryId);
                assertThat(command.principal()).isEqualTo(principal);
                assertThat(command.incarnation()).isEqualTo("current");
            });
        } finally {
            upstream.stop(0);
        }
    }

    private static AsyncResponse capture(CompletableFuture<Response> response) {
        return (AsyncResponse) Proxy.newProxyInstance(AsyncResponse.class.getClassLoader(),
                new Class<?>[] {AsyncResponse.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "register" -> Map.of();
                    case "resume" -> response.complete((Response) args[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
