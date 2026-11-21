package org.pangrova.client.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.exception.PangrovaConnectionException;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.transport.client.exception.TransportConnectionException;
import org.pangrova.transport.client.security.ResultTransportSecurity;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.testing.testkit.security.TestHttpsEndpoint;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Actual client construction and immutable policy wiring, not a TLS handshake. */
class ResultTransportSecurityConfigTest {
    @TempDir Path directory;

    @Test
    void bufferedAndStreamingResultsReportThePublicTransportFailure() {
        var endpoint = new ResultEndpoint("127.0.0.1", 1, "unused", ResultProtocol.GRPC.value());
        try (var client = PangrovaRestClient.builder()
                .resultTransportSecurity(ResultTransportSecurity.systemTrust(true)).build()) {
            var buffered = assertThrows(PangrovaConnectionException.class,
                    () -> client.fetchResult(endpoint));
            var streaming = assertThrows(PangrovaConnectionException.class,
                    () -> client.fetchResultStream(endpoint, batch -> {
                        throw new AssertionError("Plaintext endpoint delivered a batch");
                    }));
            for (var failure : List.of(buffered, streaming)) {
                assertThat(failure.errorCode()).isEqualTo("TRANSPORT_TLS_REQUIRED");
                assertThat(failure.getCause()).isInstanceOf(TransportConnectionException.class);
            }
        }
    }
    @Test
    void resultMutualTlsMaterialDoesNotChangeRestSchemeOrBecomeJavaTrustStore() {
        var result = new ResultTransportSecurity(true, Path.of("root.pem"), Path.of("client.pem"), Path.of("client-key.pem"));
        try (var client = PangrovaRestClient.builder().resultTransportSecurity(result).build()) {
            assertThat(client.config().scheme()).isEqualTo("http");
            assertThat(client.config().trustStorePath()).isNull();
            assertThat(client.config().resultTransportSecurity()).isEqualTo(result);
            assertThat(client.config().toString()).doesNotContain("root.pem", "client.pem", "client-key.pem");
        }
    }

    @Test
    void httpsClientCannotWeakenResultTlsRequirement() {
        try (var client = PangrovaRestClient.builder().useTls(true)
                .resultTransportSecurity(ResultTransportSecurity.systemTrust(false)).build()) {
            assertThat(client.config().scheme()).isEqualTo("https");
            assertThat(client.config().resultTransportSecurity().requireTls()).isTrue();
        }
    }

    @Test
    void restTrustStoreIsNeverCopiedIntoResultPemPolicy() throws Exception {
        Path store = directory.resolve("rest-private.jks");
        try (var authority = new TestTlsAuthority(directory)) {
            TestHttpsEndpoint.writeTrustStore(store, authority.root(), "test-store".toCharArray());
            try (var client = PangrovaRestClient.builder().useTls(true)
                    .trustStorePath(store.toString()).trustStorePassword("test-store").build()) {
                assertThat(client.config().trustStorePath()).isEqualTo(store.toString());
                assertThat(client.config().resultTransportSecurity()).isEqualTo(ResultTransportSecurity.systemTrust(true));
                assertThat(client.config().resultTransportSecurity().rootCaPath()).isNull();
            }
        }
    }
}
