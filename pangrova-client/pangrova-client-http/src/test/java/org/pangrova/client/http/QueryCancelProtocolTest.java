package org.pangrova.client.http;

import org.pangrova.common.client.enums.CancelOutcome;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.exception.PangrovaProtocolException;
import org.pangrova.common.client.exception.PangrovaServerException;
import org.pangrova.common.client.session.RetryPolicy;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual HTTP protocol-input validation; replies do not simulate query execution, authentication or physical drain. */
class QueryCancelProtocolTest {
    private static final String QUERY_ID = "issued-query";

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void missingQueryIdentityIsRejectedBeforeAnyNetworkRequest(String queryId) throws Exception {
        try (var peer = new CancelReply(202, reply(QUERY_ID, "CANCEL_ACCEPTED", true)); var client = peer.client()) {
            assertThatThrownBy(() -> client.cancelQuery(queryId)).isInstanceOf(IllegalArgumentException.class);
            assertThat(peer.requests.get()).isZero();
        }
    }

    @ParameterizedTest
    @CsvSource({"200,ALREADY_TERMINAL,false", "202,CANCEL_ACCEPTED,true", "404,NOT_FOUND,false"})
    void canonicalReplyRetainsOnlyItsLogicalOutcome(int status, String outcome, boolean cancelled) throws Exception {
        try (var peer = new CancelReply(status, reply(QUERY_ID, outcome, cancelled));
             var client = peer.client()) {
            assertThat(client.cancelQuery(QUERY_ID)).isEqualTo(CancelOutcome.valueOf(outcome));
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @CsvSource({"401,UNAUTHENTICATED", "403,ACCESS_DENIED", "500,INTERNAL_ERROR"})
    void actualHttpErrorsCannotBecomeSuccessfulCancellationOutcomes(int status, String errorCode) throws Exception {
        String body = "{\"error_code\":\"%s\",\"message\":\"request rejected\"}".formatted(errorCode);
        try (var peer = new CancelReply(status, body); var client = peer.client()) {
            assertThatThrownBy(() -> client.cancelQuery(QUERY_ID)).isInstanceOf(PangrovaClientException.class)
                    .satisfies(failure -> {
                        assertThat(((PangrovaClientException) failure).errorCode()).isEqualTo(errorCode);
                        if (status == 500) assertThat(failure).isInstanceOf(PangrovaServerException.class);
                    });
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 500})
    void nonJsonHttpErrorsAreClassifiedBeforeCancellationBodyParsing(int status) throws Exception {
        try (var peer = new CancelReply(status, "request rejected by upstream"); var client = peer.client()) {
            assertThatThrownBy(() -> client.cancelQuery(QUERY_ID)).isInstanceOf(PangrovaClientException.class)
                    .satisfies(failure -> assertThat(((PangrovaClientException) failure).errorCode())
                            .isEqualTo(status == 500 ? "SERVER_ERROR" : "HTTP_" + status));
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @MethodSource("untrustedReplies")
    void malformedOrConflictingActualRepliesAreRejected(int status, String body) throws Exception {
        try (var peer = new CancelReply(status, body); var client = peer.client()) {
            assertThatThrownBy(() -> client.cancelQuery(QUERY_ID)).isInstanceOf(PangrovaProtocolException.class)
                    .satisfies(failure -> {
                        var protocolFailure = (PangrovaProtocolException) failure;
                        assertThat(protocolFailure.errorCode()).isEqualTo("CANCEL_RESPONSE_INVALID");
                        assertThat(protocolFailure.httpStatusCode()).isEqualTo(status);
                    });
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    static Stream<Arguments> untrustedReplies() {
        return Stream.of(
                Arguments.of(202, ""), Arguments.of(202, "not-json"), Arguments.of(202, "null"),
                Arguments.of(202, "[]"), Arguments.of(202, "{}"),
                Arguments.of(202, reply("other-query", "CANCEL_ACCEPTED", true)),
                Arguments.of(202, reply(QUERY_ID, "CANCEL_ACCEPTED", false)),
                Arguments.of(202, reply(QUERY_ID, "UNKNOWN", true)),
                Arguments.of(202, reply(QUERY_ID, "ALREADY_TERMINAL", false)),
                Arguments.of(202, reply(QUERY_ID, "CANCEL_ACCEPTED", true) + " {}"),
                Arguments.of(202, "{\"query_id\":\"other-query\",\"query_id\":\"issued-query\",\"outcome\":\"CANCEL_ACCEPTED\",\"cancelled\":true}"),
                Arguments.of(200, reply(QUERY_ID, "CANCEL_ACCEPTED", true)),
                Arguments.of(202, "{\"query_id\":\"issued-query\",\"outcome\":\"CANCEL_ACCEPTED\",\"cancelled\":\"true\"}"),
                Arguments.of(404, "proxy route missing"), Arguments.of(404, "{\"outcome\":\"NOT_FOUND\"}"),
                Arguments.of(404, reply("other-query", "NOT_FOUND", false)),
                Arguments.of(404, reply(QUERY_ID, "NOT_FOUND", true)),
                Arguments.of(404, reply(QUERY_ID, "ALREADY_TERMINAL", false)),
                Arguments.of(204, ""));
    }

    private static String reply(String queryId, String outcome, boolean cancelled) {
        return "{\"query_id\":\"%s\",\"cancelled\":%s,\"outcome\":\"%s\"}".formatted(queryId, cancelled, outcome);
    }

    private static final class CancelReply implements AutoCloseable {
        private final ExecutorService executor = ManagedExecutorFactory.newSingleThreadExecutor("actual-cancel-protocol-peer");
        private final HttpServer server;
        private final AtomicInteger requests = new AtomicInteger();

        CancelReply(int status, String body) throws Exception {
            HttpServer allocated = null;
            try {
                allocated = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                allocated.setExecutor(executor);
                allocated.createContext("/v1/query/" + QUERY_ID + "/cancel", exchange -> {
                    try {
                        assertThat(exchange.getRequestMethod()).isEqualTo("POST");
                        assertThat(exchange.getRequestBody().readAllBytes()).isEmpty();
                        requests.incrementAndGet();
                        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
                        if (bytes.length > 0) exchange.getResponseBody().write(bytes);
                    } finally { exchange.close(); }
                });
                allocated.start();
                server = allocated;
            } catch (Exception | Error failure) {
                try { if (allocated != null) allocated.stop(0); }
                catch (RuntimeException cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                try { ExecutorDrain.shutdownAndAwait(executor, 5_000); }
                catch (RuntimeException cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        PangrovaRestClient client() {
            return PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(server.getAddress().getPort()).retryPolicy(RetryPolicy.NONE)
                    .httpExecutorThreads(1).build();
        }

        @Override public void close() {
            RuntimeException failure = null;
            try { server.stop(0); }
            catch (RuntimeException cleanupFailure) { failure = cleanupFailure; }
            try { ExecutorDrain.shutdownAndAwait(executor, 5_000); }
            catch (RuntimeException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
            if (failure != null) throw failure;
        }
    }
}
