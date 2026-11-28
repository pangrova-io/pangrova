package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.transport.http.client.HttpClientConfig;
import org.pangrova.transport.http.client.HttpClientFactory;
import org.pangrova.transport.http.client.ManagedHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;

/** Routes authenticated query controls once using an owner-issued ticket. */
public final class QueryPeerRouting implements AutoCloseable {
    static final String PATH = "v1/internal/query-peer";
    static final String SIGNATURE = "X-Pangrova-Query-Peer-Signature";
    private static final Logger LOG = LoggerFactory.getLogger(QueryPeerRouting.class);
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final int MAX_TICKET_LENGTH = 4096;
    static final int MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
    private static final int MAX_IN_FLIGHT = 512;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    static final int MAX_COMMAND_BYTES = 8 * 1024;
    private static final long DELIVERY_MS = 2_000;
    private static final long CLOCK_SKEW_MS = 1_000;
    private static final long MAX_COMMAND_LIFETIME_MS = DELIVERY_MS + QueryStatusResponse.MAX_WAIT_MILLIS;
    private static final byte TICKET_DOMAIN = 1;
    private static final byte COMMAND_DOMAIN = 2;
    private static final byte RESPONSE_DOMAIN = 3;

    enum Operation { STATE, CANCEL, PROFILE, FLAME_GRAPH }

    record Observation(long afterVersion, long waitMillis, QueryWaitTarget target, String observer) {
    }

    record Route(QueryOwnerEndpoint owner, String queryId, String principal) {
    }

    record Command(String coordinatorId, String incarnation, String queryId, String principal,
            Operation operation, Observation observation, UUID requestId, long expiresAtMs) {
    }

    private final byte[] key;
    private final ManagedHttpClient client;
    private final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);

    public QueryPeerRouting(String signingKey) {
        key = signingKey.getBytes(StandardCharsets.UTF_8);
        client = HttpClientFactory.create(new HttpClientConfig("query-peer", 64,
                Duration.ofMillis(DELIVERY_MS), Duration.ofMillis(MAX_COMMAND_LIFETIME_MS)));
    }

    QueryOwnerEndpoint issue(QueryOwnerEndpoint owner, String queryId, String principal) {
        byte[] body = encode(new Route(owner, queryId, principal));
        String ticket = ENCODER.encodeToString(body) + "." + sign(TICKET_DOMAIN, body);
        if (ticket.length() > MAX_TICKET_LENGTH) {
            throw new IllegalArgumentException("Query route exceeds the ticket limit");
        }
        return new QueryOwnerEndpoint(owner.coordinatorId(), owner.incarnation(), owner.host(), owner.port(), ticket);
    }

    boolean forward(String ticket, String queryId, String principal, QueryOwnerEndpoint local,
            Operation operation, Observation observation, AsyncResponse response) {
        Route route = readRoute(ticket);
        if (route == null || !queryId.equals(route.queryId()) || !principal.equals(route.principal())
                || local.coordinatorId().equals(route.owner().coordinatorId())) {
            return false;
        }
        if (!inFlight.tryAcquire()) {
            response.resume(error(HTTP_TOO_MANY_REQUESTS, "QUERY_PEER_LIMIT", "Query peer capacity is exhausted"));
            return true;
        }
        try {
            send(route, operation, observation, response);
        } catch (RuntimeException failure) {
            inFlight.release();
            throw failure;
        }
        return true;
    }

    private void send(Route route, Operation operation, Observation observation, AsyncResponse response) {
        long timeoutMs = DELIVERY_MS + (observation == null ? 0 : observation.waitMillis());
        var owner = route.owner();
        var command = new Command(owner.coordinatorId(), owner.incarnation(), route.queryId(), route.principal(),
                operation, observation, UUID.randomUUID(), System.currentTimeMillis() + timeoutMs);
        byte[] body = encode(command);
        String signature = sign(COMMAND_DOMAIN, body);
        var request = HttpRequest.newBuilder(owner.origin("http").resolve("/" + PATH))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .header(SIGNATURE, signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        var exchange = client.httpClient().sendAsync(request, ignored -> new LimitedBody());
        try {
            response.register((CompletionCallback) failure -> exchange.cancel(true),
                    (ConnectionCallback) disconnected -> exchange.cancel(true));
        } catch (RuntimeException failure) {
            exchange.cancel(true);
            throw failure;
        }
        exchange.copy().orTimeout(timeoutMs, TimeUnit.MILLISECONDS).whenComplete((reply, failure) -> {
            try {
                if (failure != null) {
                    exchange.cancel(true);
                    LOG.debug("event=query_peer_unavailable queryId={}", route.queryId(), failure);
                    response.resume(unavailable());
                } else {
                    response.resume(relay(reply, signature, operation));
                }
            } finally {
                inFlight.release();
            }
        });
    }

    private Response relay(HttpResponse<byte[]> reply, String requestSignature, Operation operation) {
        int status = reply.statusCode();
        String signature = reply.headers().firstValue(SIGNATURE).orElse(null);
        var family = Response.Status.Family.familyOf(status);
        if (family == Response.Status.Family.INFORMATIONAL || family == Response.Status.Family.REDIRECTION
                || family == Response.Status.Family.OTHER
                || !matches(responseSignature(requestSignature, status, reply.body()), signature)) {
            return unavailable();
        }
        return Response.status(status).type(mediaType(operation, status)).entity(reply.body()).build();
    }

    Command authenticate(byte[] body, String signature, QueryOwnerEndpoint local) throws IOException {
        if (body.length > MAX_COMMAND_BYTES || !matches(sign(COMMAND_DOMAIN, body), signature)) {
            return null;
        }
        var command = JsonMapperFactory.shared().readValue(body, Command.class);
        long now = System.currentTimeMillis();
        if (command == null || !local.coordinatorId().equals(command.coordinatorId())
                || !local.incarnation().equals(command.incarnation())
                || command.expiresAtMs() <= now - CLOCK_SKEW_MS
                || command.expiresAtMs() - now > MAX_COMMAND_LIFETIME_MS + CLOCK_SKEW_MS
                || command.queryId() == null || command.principal() == null || command.principal().isBlank()
                || command.requestId() == null || command.operation() == null
                || (command.operation() == Operation.STATE) != (command.observation() != null)) {
            return null;
        }
        return command;
    }

    String responseSignature(String requestSignature, int status, byte[] body) {
        Mac mac = mac(RESPONSE_DOMAIN);
        mac.update(requestSignature.getBytes(StandardCharsets.US_ASCII));
        mac.update(ByteBuffer.allocate(Integer.BYTES).putInt(status).array());
        return ENCODER.encodeToString(mac.doFinal(body));
    }

    static String mediaType(Operation operation, int status) {
        return operation == Operation.FLAME_GRAPH && status == Response.Status.OK.getStatusCode()
                ? "image/svg+xml" : MediaType.APPLICATION_JSON;
    }

    static byte[] encode(Object message) {
        try {
            return JsonMapperFactory.writer().writeValueAsBytes(message);
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot encode query peer message", failure);
        }
    }

    boolean authenticatesReceipt(String ticket, String queryId, String principal) {
        Route route = readRoute(ticket);
        return route != null && queryId.equals(route.queryId()) && principal.equals(route.principal());
    }

    private Route readRoute(String ticket) {
        if (ticket == null || ticket.length() > MAX_TICKET_LENGTH) return null;
        int split = ticket.indexOf('.');
        if (split < 1) return null;
        try {
            byte[] body = DECODER.decode(ticket.substring(0, split));
            if (!matches(sign(TICKET_DOMAIN, body), ticket.substring(split + 1))) return null;
            Route route = JsonMapperFactory.shared().readValue(body, Route.class);
            return route == null || route.owner() == null ? null : route;
        } catch (IOException | IllegalArgumentException invalid) {
            return null;
        }
    }

    private String sign(byte domain, byte[] body) {
        return ENCODER.encodeToString(mac(domain).doFinal(body));
    }

    private Mac mac(byte domain) {
        Mac mac = Sha256Digests.hmacSha256(key);
        mac.update(domain);
        return mac;
    }

    private static boolean matches(String actual, String received) {
        return received != null && actual.length() == received.length() && MessageDigest.isEqual(
                actual.getBytes(StandardCharsets.US_ASCII), received.getBytes(StandardCharsets.US_ASCII));
    }

    private static Response unavailable() {
        return error(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(),
                "QUERY_OWNER_UNAVAILABLE", "Query owner is unavailable");
    }

    private static Response error(int status, String code, String message) {
        return Response.status(status).entity(new ErrorDetail(code, null, message)).build();
    }

    @Override
    public void close() {
        client.close();
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> body = HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private long received;

        @Override public CompletionStage<byte[]> getBody() { return body.getBody(); }

        @Override public void onSubscribe(Flow.Subscription incoming) {
            subscription = incoming;
            body.onSubscribe(incoming);
        }

        @Override public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                received += buffer.remaining();
            }
            if (received > MAX_RESPONSE_BYTES) {
                subscription.cancel();
                body.onError(new IOException("Query peer response exceeds its byte limit"));
            } else {
                body.onNext(buffers);
            }
        }

        @Override public void onError(Throwable failure) { body.onError(failure); }
        @Override public void onComplete() { body.onComplete(); }
    }
}
