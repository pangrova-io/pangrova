package org.pangrova.transport.security;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultTransportSecurityTest {
    private static final Path CA = Path.of("private-root.pem");
    private static final Path CHAIN = Path.of("private-chain.pem");
    private static final Path KEY = Path.of("private-key.pem");

    @Test
    void systemTrustAndMaterialPoliciesRetainTlsRequirements() {
        var allowed = ResultTransportSecurity.systemTrust(false);
        assertThat(allowed.requireTls()).isFalse();
        assertThat(allowed.requiringTls(true).requiringTls(false).requireTls()).isTrue();
        var configured = new ResultTransportSecurity(false, CA, CHAIN, KEY);
        assertThat(configured.requireTls()).isTrue();
        assertThat(configured.requiringTls(false)).isEqualTo(configured);
        assertThat(new ResultTransportSecurity(false, CA, null, null).requireTls()).isTrue();
    }

    @Test
    void incompleteMaterialAndEmptyPathsAreRejected() {
        assertThatThrownBy(() -> new ResultTransportSecurity(true, CA, CHAIN, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResultTransportSecurity(true, CA, null, KEY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResultTransportSecurity(true, null, CHAIN, KEY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResultTransportSecurity(true, Path.of(""), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void diagnosticsNeverRenderMaterialPaths() {
        assertThat(new ResultTransportSecurity(true, CA, CHAIN, KEY).toString())
                .contains("privateKey=<redacted>")
                .doesNotContain(CA.toString(), CHAIN.toString(), KEY.toString());
    }
}
