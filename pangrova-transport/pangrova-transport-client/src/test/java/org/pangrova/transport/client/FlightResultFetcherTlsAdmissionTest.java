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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.client.protocol.ResultProtocol;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

/** Transport-policy rejection tests; these do not claim a successful TLS handshake. */
class FlightResultFetcherTlsAdmissionTest {
    @TempDir Path materialDirectory;

    @Test
    void plaintextAndTlsEndpointsCannotShareAClientPoolEntry() {
        assertThat(FlightResultFetcher.poolKey(endpoint(ResultProtocol.GRPC)))
                .isNotEqualTo(FlightResultFetcher.poolKey(endpoint(ResultProtocol.GRPC_TLS)));
    }

    @Test
    void tlsRequiredRejectsActualPlaintextEndpointBeforeConnecting() {
        try (var fetcher = new FlightResultFetcher(ResultTransportSecurity.systemTrust(true))) {
            assertThatThrownBy(() -> fetcher.fetch(endpoint(ResultProtocol.GRPC)))
                    .isInstanceOf(TransportConnectionException.class)
                    .satisfies(error -> org.assertj.core.api.Assertions.assertThat(
                            ((TransportConnectionException) error).transportErrorCode())
                            .isEqualTo("TRANSPORT_TLS_REQUIRED"));
        }
    }

    @Test
    void streamingHasTheSameTlsAdmissionWithoutCallingConsumer() {
        try (var fetcher = new FlightResultFetcher(ResultTransportSecurity.systemTrust(true))) {
            assertThatThrownBy(() -> fetcher.fetchStream(endpoint(ResultProtocol.GRPC),
                    batch -> { throw new AssertionError("Rejected endpoint cannot produce a batch"); }))
                    .isInstanceOf(TransportConnectionException.class)
                    .hasMessageContaining("TLS is required");
        }
    }

    @Test
    void tlsDescriptorDoesNotBypassUnreadablePemPolicy() {
        var policy = new ResultTransportSecurity(false, materialDirectory.resolve("private-root.pem"), null, null);
        try (var fetcher = new FlightResultFetcher(policy)) {
            assertThatThrownBy(() -> fetcher.fetch(endpoint(ResultProtocol.GRPC_TLS)))
                    .isInstanceOf(TransportConnectionException.class)
                    .satisfies(error -> assertThat(((TransportConnectionException) error).transportErrorCode())
                            .isEqualTo("TRANSPORT_TLS_CONFIGURATION_INVALID"))
                    .hasMessageNotContaining("private-root.pem");
        }
    }

    @Test
    void requiredTlsRejectsMalformedPemMaterialWithoutPretendingInitializationSucceeded() throws Exception {
        Path invalid = Files.writeString(materialDirectory.resolve("invalid-root.pem"), "not a PEM certificate");
        try (var fetcher = new FlightResultFetcher(new ResultTransportSecurity(true, invalid, null, null))) {
            assertThatThrownBy(() -> fetcher.fetch(endpoint(ResultProtocol.GRPC_TLS)))
                    .isInstanceOf(TransportConnectionException.class)
                    .satisfies(error -> assertThat(((TransportConnectionException) error).transportErrorCode())
                            .isEqualTo("TRANSPORT_TLS_CONFIGURATION_INVALID"));
        }
    }

    private static ResultEndpoint endpoint(ResultProtocol protocol) {
        return new ResultEndpoint("127.0.0.1", 1, "policy-rejection", protocol.value());
    }
}
