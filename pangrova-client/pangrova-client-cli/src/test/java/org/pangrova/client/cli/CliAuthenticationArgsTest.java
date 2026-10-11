package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;
import org.pangrova.client.rest.auth.ClientAuthentication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The actual CLI option parser, not a simulated successful SQL client. */
class CliAuthenticationArgsTest {
    @Test
    void typedCredentialModesReachTheSameParsedConfiguration() {
        var basic = PangrovaCli.parseArgs(new String[]{"--user", "读者", "--password", "密碼:secret"});
        assertThat(basic.authentication.authorizationHeader())
                .isEqualTo(ClientAuthentication.basic("读者", "密碼:secret").authorizationHeader());
        assertThat(PangrovaCli.parseArgs(new String[]{"--token", "signed-token"}).authentication.authorizationHeader())
                .isEqualTo("Bearer signed-token");
        assertThat(PangrovaCli.parseArgs(new String[]{}).authentication).isNull();
    }

    @Test
    void missingAndConflictingAuthOptionsCannotSilentlySelectAnonymous() {
        for (String[] args : new String[][]{{"--token"}, {"--user", "reader"}, {"--password", "secret"},
                {"--token", "signed", "--user", "reader", "--password", "secret"},
                {"--token", "signed", "--token-env", "PANGROVA_TOKEN"},
                {"--token-env", "PANGROVA_TOKEN", "--user", "reader", "--password", "secret"}}) {
            assertThatThrownBy(() -> PangrovaCli.parseArgs(args)).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void environmentTokenSourcesCannotBecomeAnonymousOrRevealValues() {
        for (String name : new String[]{"", "invalid-name", "PANGROVA_CLI_TEST_ABSENT_TOKEN_7C427804"}) {
            assertThatThrownBy(() -> PangrovaCli.parseArgs(new String[]{"--token-env", name}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Bearer token environment variable");
        }
    }

    @Test
    void credentialConnectionsCanSelectTheExistingTlsClientConfiguration() {
        var args = PangrovaCli.parseArgs(new String[]{"--ssl", "true", "--ssl-trust-store", "trusted.jks",
                "--ssl-trust-store-password", "store-password", "--user", "reader", "--password", "secret"});
        assertThat(args.tls.enabled()).isTrue();
        assertThat(args.tls.storePath()).isEqualTo("trusted.jks");
        assertThat(args.tls.storePassword()).isEqualTo("store-password");
        assertThatThrownBy(() -> PangrovaCli.parseArgs(new String[]{"--ssl", "maybe"}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
