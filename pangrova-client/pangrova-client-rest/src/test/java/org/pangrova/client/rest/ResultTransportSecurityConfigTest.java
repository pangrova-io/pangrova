package org.pangrova.client.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.transport.security.ResultTransportSecurity;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.testing.testkit.security.TestHttpsEndpoint;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual client construction and immutable policy wiring, not a TLS handshake. */
class ResultTransportSecurityConfigTest {
    @TempDir Path directory;
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
