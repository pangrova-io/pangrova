/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.transport.client;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Flight transport errors are classified using structured
 * {@link FlightStatusCode} rather than brittle message substring matching.
 *
 * <p>Validates fix for F-COMMON-10: Flight transport errors must be classified
 * by gRPC/Flight status codes (structured, numeric) instead of error message text.</p>
 */
@DisplayName("FlightResultFetcher error classification (F-COMMON-10)")
class FlightResultFetcherErrorClassificationTest {

    private static final ResultEndpoint ENDPOINT = new ResultEndpoint(
            "worker-1", 9090, "ticket-abc", "grpc");

    // ── Structured FlightStatusCode classification ──

    @Nested
    @DisplayName("FlightRuntimeException with structured status codes")
    class StructuredFlightStatusCodes {

        @Test
        @DisplayName("UNAVAILABLE -> TransportConnectionException (retryable)")
        void unavailableStatusClassifiedAsConnection() {
            FlightRuntimeException ex = CallStatus.UNAVAILABLE
                    .withDescription("server shutting down")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_UNAVAILABLE");
            assertThat(result.getCause()).isSameAs(ex);
        }

        @Test
        @DisplayName("TIMED_OUT -> TransportConnectionException (retryable)")
        void timedOutStatusClassifiedAsConnection() {
            FlightRuntimeException ex = CallStatus.TIMED_OUT
                    .withDescription("deadline exceeded")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_TIMED_OUT");
            assertThat(result.getCause()).isSameAs(ex);
        }

        @Test
        @DisplayName("NOT_FOUND -> TransportConnectionException (endpoint missing)")
        void notFoundStatusClassifiedAsConnection() {
            FlightRuntimeException ex = CallStatus.NOT_FOUND
                    .withDescription("stream not found")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_NOT_FOUND");
        }

        @Test
        @DisplayName("RESOURCE_EXHAUSTED -> TransportServerException with 503")
        void resourceExhaustedClassifiedAsServer() {
            FlightRuntimeException ex = CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("memory limit exceeded")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            TransportServerException serverEx = (TransportServerException) result;
            assertThat(serverEx.transportErrorCode()).isEqualTo("TRANSPORT_RESOURCE_EXHAUSTED");
            assertThat(serverEx.statusCode()).isEqualTo(503);
        }

        @Test
        @DisplayName("UNAUTHENTICATED -> TransportServerException with 401")
        void unauthenticatedClassifiedAsServer() {
            FlightRuntimeException ex = CallStatus.UNAUTHENTICATED
                    .withDescription("token expired")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            TransportServerException serverEx = (TransportServerException) result;
            assertThat(serverEx.transportErrorCode()).isEqualTo("TRANSPORT_UNAUTHENTICATED");
            assertThat(serverEx.statusCode()).isEqualTo(401);
        }

        @Test
        @DisplayName("UNAUTHORIZED -> TransportServerException with 403")
        void unauthorizedClassifiedAsServer() {
            FlightRuntimeException ex = CallStatus.UNAUTHORIZED
                    .withDescription("insufficient permissions")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            TransportServerException serverEx = (TransportServerException) result;
            assertThat(serverEx.transportErrorCode()).isEqualTo("TRANSPORT_UNAUTHORIZED");
            assertThat(serverEx.statusCode()).isEqualTo(403);
        }

        @Test
        @DisplayName("CANCELLED -> TransportServerException")
        void cancelledClassifiedAsServer() {
            FlightRuntimeException ex = CallStatus.CANCELLED
                    .withDescription("client cancelled")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_CANCELLED");
        }

        @Test
        @DisplayName("INTERNAL -> TransportServerException (default)")
        void internalClassifiedAsServerDefault() {
            FlightRuntimeException ex = CallStatus.INTERNAL
                    .withDescription("unexpected error")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_ERROR");
            assertThat(result.getMessage()).contains("INTERNAL");
        }

        @Test
        @DisplayName("UNKNOWN -> TransportServerException (default)")
        void unknownClassifiedAsServerDefault() {
            FlightRuntimeException ex = CallStatus.UNKNOWN
                    .withDescription("something went wrong")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_ERROR");
            assertThat(result.getMessage()).contains("UNKNOWN");
        }

        @Test
        @DisplayName("INVALID_ARGUMENT -> TransportServerException (default)")
        void invalidArgumentClassifiedAsServerDefault() {
            FlightRuntimeException ex = CallStatus.INVALID_ARGUMENT
                    .withDescription("bad ticket format")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_ERROR");
        }
    }

    // ── Status code does NOT depend on message content (anti-regression) ──

    @Nested
    @DisplayName("Classification is message-independent (anti-regression for F-COMMON-10)")
    class MessageIndependence {

        @Test
        @DisplayName("INTERNAL with 'unavailable' in message is NOT classified as connection error")
        void internalWithUnavailableInMessageNotMisclassified() {
            FlightRuntimeException ex = CallStatus.INTERNAL
                    .withDescription("query references table 'unavailable_items'")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result).isNotInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("INTERNAL with 'Connection' in message is NOT classified as connection error")
        void internalWithConnectionInMessageNotMisclassified() {
            FlightRuntimeException ex = CallStatus.INTERNAL
                    .withDescription("error in ConnectionPool statistics query")
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result).isNotInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("UNAVAILABLE with empty message still classified as connection error")
        void unavailableWithEmptyMessageStillConnection() {
            FlightRuntimeException ex = new CallStatus(FlightStatusCode.UNAVAILABLE)
                    .toRuntimeException();

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_UNAVAILABLE");
        }
    }

    // ── FlightRuntimeException wrapped in another exception ──

    @Nested
    @DisplayName("Wrapped FlightRuntimeException (cause chain extraction)")
    class WrappedFlightException {

        @Test
        @DisplayName("FlightRuntimeException wrapped in IllegalStateException is extracted by status code")
        void wrappedFlightExceptionExtracted() {
            FlightRuntimeException flightEx = CallStatus.UNAVAILABLE
                    .withDescription("server unavailable")
                    .toRuntimeException();
            IllegalStateException wrapper = new IllegalStateException("DoGet failed", flightEx);

            FlightStatusCode code = FlightResultFetcher.extractFlightStatusCode(wrapper);
            assertThat(code).isEqualTo(FlightStatusCode.UNAVAILABLE);
        }

        @Test
        @DisplayName("Deeply nested FlightRuntimeException is extracted (up to depth 5)")
        void deeplyNestedFlightExceptionExtracted() {
            FlightRuntimeException flightEx = CallStatus.TIMED_OUT
                    .withDescription("deadline exceeded")
                    .toRuntimeException();
            Exception level1 = new RuntimeException("level 1", flightEx);
            Exception level2 = new RuntimeException("level 2", level1);
            Exception level3 = new RuntimeException("level 3", level2);

            FlightStatusCode code = FlightResultFetcher.extractFlightStatusCode(level3);
            assertThat(code).isEqualTo(FlightStatusCode.TIMED_OUT);
        }

        @Test
        @DisplayName("No FlightRuntimeException in chain returns null")
        void noFlightExceptionReturnsNull() {
            Exception plain = new IOException("network error");
            FlightStatusCode code = FlightResultFetcher.extractFlightStatusCode(plain);
            assertThat(code).isNull();
        }
    }

    // ── Fallback: non-Flight exceptions classified by type ──

    @Nested
    @DisplayName("Non-Flight exception fallback (type-based classification)")
    class NonFlightFallback {

        @Test
        @DisplayName("ConnectException -> TransportConnectionException")
        void connectExceptionClassifiedAsConnection() {
            ConnectException ex = new ConnectException("Connection refused");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_CONNECTION_FAILED");
        }

        @Test
        @DisplayName("SocketTimeoutException -> TransportConnectionException")
        void socketTimeoutClassifiedAsConnection() {
            SocketTimeoutException ex = new SocketTimeoutException("Read timed out");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("UnknownHostException -> TransportConnectionException")
        void unknownHostClassifiedAsConnection() {
            UnknownHostException ex = new UnknownHostException("worker-1.invalid");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("NoRouteToHostException -> TransportConnectionException")
        void noRouteClassifiedAsConnection() {
            NoRouteToHostException ex = new NoRouteToHostException("No route to host");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("Generic IOException -> TransportConnectionException")
        void ioExceptionClassifiedAsConnection() {
            IOException ex = new IOException("Broken pipe");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportConnectionException.class);
        }

        @Test
        @DisplayName("EOFException -> TransportServerException (not a network error)")
        void eofExceptionClassifiedAsServerError() {
            EOFException ex = new EOFException("unexpected end of stream");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_ERROR");
        }

        @Test
        @DisplayName("Generic RuntimeException -> TransportServerException")
        void runtimeExceptionClassifiedAsServer() {
            RuntimeException ex = new RuntimeException("unexpected error");

            TransportException result = FlightResultFetcher.classifyError(ENDPOINT, ex);

            assertThat(result).isInstanceOf(TransportServerException.class);
            assertThat(result.transportErrorCode()).isEqualTo("TRANSPORT_ERROR");
        }
    }

    // ── extractFlightStatusCode covers all codes ──

    @Nested
    @DisplayName("extractFlightStatusCode utility")
    class ExtractStatusCode {

        @ParameterizedTest
        @EnumSource(value = FlightStatusCode.class, names = "OK", mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("All non-OK FlightStatusCodes are extractable")
        void allNonOkCodesExtracted(FlightStatusCode code) {
            FlightRuntimeException ex = new CallStatus(code)
                    .withDescription("test")
                    .toRuntimeException();

            FlightStatusCode extracted = FlightResultFetcher.extractFlightStatusCode(ex);
            assertThat(extracted).isEqualTo(code);
        }

        @Test
        @DisplayName("Null exception returns null")
        void nullReturnsNull() {
            FlightStatusCode code = FlightResultFetcher.extractFlightStatusCode(null);
            assertThat(code).isNull();
        }
    }
}
