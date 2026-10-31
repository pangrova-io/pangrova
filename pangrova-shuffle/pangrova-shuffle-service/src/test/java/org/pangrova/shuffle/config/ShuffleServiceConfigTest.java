package org.pangrova.shuffle.config;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleServiceConfigTest {

    @Test
    void rejectsUnknownSecurityKey() {
        Map<String, Object> root = validConfig();
        root.put("security", Map.of("certificate-chain-path", "/cert",
                "private-key-path", "/key", "root-ca-path", "/ca",
                "capability-secret-path", "/secret", "coordinator-tls-identities",
                List.of("spiffe://pangrova/coordinator"), "capabilty-ttl-seconds", 60));

        assertThatThrownBy(() -> ShuffleServiceConfig.from(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown key");
    }

    @Test
    void rejectsStringThatLooksLikeBoolean() {
        Map<String, Object> root = validConfig();
        root.put("object-storage", Map.of("bucket", "shuffle",
                "path-style-access", "ture"));

        assertThatThrownBy(() -> ShuffleServiceConfig.from(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a boolean");
    }

    @Test
    void parsesTypedSecurityAndCapacityLimits() {
        ShuffleServiceConfig config = ShuffleServiceConfig.from(validConfig());

        assertThat(config.security().coordinatorTlsIdentities())
                .containsExactly("spiffe://pangrova/coordinator");
        assertThat((long) config.limits().maxInflightBytes()).isGreaterThanOrEqualTo(
                3L * config.limits().maxBatchBytes());
        assertThat(config.limits().maxQueuedOperations())
                .isGreaterThanOrEqualTo(config.limits().maxWriteStreams());
    }

    @Test
    void parsesBoundedObjectStorageApiTimeouts() {
        Map<String, Object> root = validConfig();
        root.put("object-storage", Map.of("bucket", "shuffle",
                "api-call-timeout-seconds", 90,
                "api-call-attempt-timeout-seconds", 30));

        ShuffleServiceConfig config = ShuffleServiceConfig.from(root);
        assertThat(config.objectStorage().apiCallTimeout()).hasSeconds(90);
        assertThat(config.objectStorage().apiCallAttemptTimeout()).hasSeconds(30);
    }

    @Test
    void rejectsAttemptTimeoutLongerThanWholeApiCall() {
        Map<String, Object> root = validConfig();
        root.put("object-storage", Map.of("bucket", "shuffle",
                "api-call-timeout-seconds", 30,
                "api-call-attempt-timeout-seconds", 60));

        assertThatThrownBy(() -> ShuffleServiceConfig.from(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid object storage API timeouts");
    }

    @Test
    void rejectsFractionalIntegerLimit() {
        Map<String, Object> root = validConfig();
        root.put("limits", Map.of("max-write-streams", 1.5));

        assertThatThrownBy(() -> ShuffleServiceConfig.from(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact 32-bit integer");
    }

    @Test
    void rejectsOverflowingIntegerLimit() {
        Map<String, Object> root = validConfig();
        root.put("limits", Map.of("max-write-streams", 2_147_483_648L));

        assertThatThrownBy(() -> ShuffleServiceConfig.from(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact 32-bit integer");
    }

    private static Map<String, Object> validConfig() {
        return new java.util.HashMap<>(Map.of(
                "server", Map.of("port", 7443, "advertised-host", "shuffle",
                        "tls-identity", "shuffle.internal"),
                "database", Map.of("jdbc-url", "jdbc:postgresql://db/shuffle",
                        "username", "shuffle", "password-file", "/password"),
                "object-storage", Map.of("bucket", "shuffle"),
                "security", Map.of("certificate-chain-path", "/cert",
                        "private-key-path", "/key", "root-ca-path", "/ca",
                        "capability-secret-path", "/secret",
                        "coordinator-tls-identities",
                        List.of("spiffe://pangrova/coordinator"))));
    }
}
