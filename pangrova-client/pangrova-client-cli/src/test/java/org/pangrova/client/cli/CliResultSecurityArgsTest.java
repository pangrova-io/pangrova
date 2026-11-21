package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CliResultSecurityArgsTest {
    @Test
    void resultPemOptionsRemainSeparateFromRestTrustStore() {
        var args = PangrovaCli.parseArgs(new String[]{"--result-tls-root-ca", "root.pem",
                "--result-tls-certificate-chain", "client.pem", "--result-tls-private-key", "key.pem",
                "--ssl-trust-store", "rest.jks"});
        assertThat(args.tls.enabled()).isFalse();
        assertThat(args.tls.storePath()).isEqualTo("rest.jks");
        assertThat(args.resultTransportSecurity.requireTls()).isTrue();
        assertThat(args.resultTransportSecurity.rootCaPath()).isEqualTo(Path.of("root.pem"));
        assertThat(args.resultTransportSecurity.certificateChainPath()).isEqualTo(Path.of("client.pem"));
        assertThat(args.resultTransportSecurity.privateKeyPath()).isEqualTo(Path.of("key.pem"));
    }

    @Test
    void partialAndMisspelledResultCredentialsAreRejected() {
        for (String[] args : new String[][]{{"--result-tls-private-key", "key.pem"},
                {"--result-tls-certificate-chain", "client.pem"}, {"--result-tls-root-ca", ""},
                {"--result-tls-client-key", "key.pem"}}) {
            assertThatThrownBy(() -> PangrovaCli.parseArgs(args)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
