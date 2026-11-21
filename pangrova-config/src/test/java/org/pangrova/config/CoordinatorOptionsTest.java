package org.pangrova.config;

import org.pangrova.config.constants.CoordinatorOptions;


import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CoordinatorOptions}.
 * Verifies all typed config keys, default values, and the R1.3 requirement
 * that all feature flags default to true except security.enabled.
 */
class CoordinatorOptionsTest {

    // ---- Server defaults ----

    @Test
    void coordinatorHostDefaultsToAllInterfaces() {
        assertThat(CoordinatorOptions.COORDINATOR_HOST.defaultValue()).isEqualTo("0.0.0.0");
        assertThat(CoordinatorOptions.COORDINATOR_HOST.typeClass()).isEqualTo(String.class);
    }

    @Test
    void coordinatorPortDefaultsTo8080() {
        assertThat(CoordinatorOptions.COORDINATOR_PORT.defaultValue()).isEqualTo(8080);
        assertThat(CoordinatorOptions.COORDINATOR_PORT.typeClass()).isEqualTo(Integer.class);
    }

    @Test
    void coordinatorTransportTlsIsExplicitAndDisabledByDefault() {
        assertThat(CoordinatorOptions.TRANSPORT_TLS_ENABLED.defaultValue()).isFalse();
        assertThat(CoordinatorOptions.TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH.defaultValue())
                .isEmpty();
        assertThat(CoordinatorOptions.TRANSPORT_TLS_PRIVATE_KEY_PATH.defaultValue())
                .isEmpty();
        assertThat(CoordinatorOptions.TRANSPORT_TLS_ROOT_CA_PATH.defaultValue())
                .isEmpty();
        assertThat(CoordinatorOptions.SHUFFLE_TLS_SERVER_IDENTITY.defaultValue())
                .isEmpty();
    }

    // ---- Feature flags: tenancy and security require explicit activation ----

    @Test
    void remainingFeatureFlagsDefaultTrue() {
        List<ConfigOption<Boolean>> enabledByDefault = List.of(
                CoordinatorOptions.METADATA_ENABLED,
                CoordinatorOptions.GOVERNANCE_ENABLED,
                CoordinatorOptions.RESILIENCE_ENABLED,
                CoordinatorOptions.STREAMING_ENABLED,
                CoordinatorOptions.INDEXING_ENABLED,
                CoordinatorOptions.OPERATIONS_ENABLED,
                CoordinatorOptions.OBSERVABILITY_ENABLED);

        for (ConfigOption<Boolean> flag : enabledByDefault) {
            assertThat(flag.defaultValue())
                    .as("Feature flag '%s' should default to true", flag.key())
                    .isTrue();
            assertThat(flag.typeClass()).isEqualTo(Boolean.class);
        }
    }

    @Test
    void securityEnabledDefaultsToFalse() {
        assertThat(CoordinatorOptions.SECURITY_ENABLED.defaultValue()).isFalse();
        assertThat(CoordinatorOptions.SECURITY_ENABLED.key()).isEqualTo("security.enabled");
        assertThat(CoordinatorOptions.SECURITY_ENABLED.typeClass()).isEqualTo(Boolean.class);
    }

    @Test
    void tenantEnabledDefaultsToFalseWithoutAuthentication() {
        assertThat(CoordinatorOptions.TENANT_ENABLED.defaultValue()).isFalse();
        assertThat(CoordinatorOptions.TENANT_ENABLED.key()).isEqualTo("tenant.enabled");
    }

    // ---- HA defaults ----

    @Test
    void internalControlSecretIsExplicitAndSpeculationIsOptIn() {
        var defaults = PangrovaConfiguration.fromYaml(Map.of());
        assertThat(defaults.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET)).isNull();
        assertThat(defaults.getBoolean(CoordinatorOptions.SPECULATION_ENABLED)).isFalse();
        var configured = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.internal-auth-secret", "configured-exact-secret",
                "coordinator.speculation.enabled", "true"));
        assertThat(configured.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET))
                .isEqualTo("configured-exact-secret");
        assertThat(configured.getBoolean(CoordinatorOptions.SPECULATION_ENABLED)).isTrue();
    }

    @Test
    void highAvailabilityEnabledDefaultsToFalse() {
        assertThat(CoordinatorOptions.HIGH_AVAILABILITY_ENABLED.defaultValue()).isFalse();
        assertThat(CoordinatorOptions.HIGH_AVAILABILITY_ENABLED.key()).isEqualTo("coordinator.ha.enabled");
    }


    // ---- Query defaults ----

    @Test
    void queryTimeoutDefaultsToFiveMinutes() {
        assertThat(CoordinatorOptions.QUERY_TIMEOUT.defaultValue()).isEqualTo(Duration.ofMinutes(5));
        assertThat(CoordinatorOptions.QUERY_TIMEOUT.typeClass()).isEqualTo(Duration.class);
    }

    @Test
    void queryMaxConcurrentDefaultsTo100() {
        assertThat(CoordinatorOptions.QUERY_MAX_CONCURRENT.defaultValue()).isEqualTo(100);
    }

    // ---- Worker management defaults ----

    @Test
    void workerHeartbeatTimeoutDefaultsTo30Seconds() {
        assertThat(CoordinatorOptions.WORKER_HEARTBEAT_TIMEOUT.defaultValue()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void workerMaxRetryDefaultsTo3() {
        assertThat(CoordinatorOptions.WORKER_MAX_RETRY.defaultValue()).isEqualTo(3);
    }

    // ---- Metadata defaults ----

    @Test
    void metadataJdbcUrlHasNoDefault() {
        assertThat(CoordinatorOptions.METADATA_JDBC_URL.defaultValue()).isNull();
        assertThat(CoordinatorOptions.METADATA_JDBC_URL.hasDefaultValue()).isFalse();
    }

    @Test
    void metadataCredentialsDefaultToEmptyAndHaveIndependentTypedKeys() {
        var config = PangrovaConfiguration.fromYaml(Map.of());
        assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_USERNAME)).isEmpty();
        assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_PASSWORD)).isEmpty();
        assertThat(CoordinatorOptions.METADATA_JDBC_USERNAME.key()).isEqualTo("metadata.jdbc-username");
        assertThat(CoordinatorOptions.METADATA_JDBC_PASSWORD.key()).isEqualTo("metadata.jdbc-password");
    }

    @Test
    void metadataCacheL1MaxSizeDefaultsTo10000() {
        assertThat(CoordinatorOptions.METADATA_CACHE_L1_MAX_SIZE.defaultValue()).isEqualTo(10_000);
    }

    @Test
    void metadataCacheL1TtlDefaultsTo60Seconds() {
        assertThat(CoordinatorOptions.METADATA_CACHE_L1_TTL.defaultValue()).isEqualTo(Duration.ofSeconds(60));
    }

    // ---- R1.3: Empty config keeps unauthenticated tenancy disabled ----

    @Test
    void emptyConfigDisablesSecurityAndTenancy() {
        PangrovaConfiguration emptyConfig = PangrovaConfiguration.fromYaml(Map.of());

        assertThat(emptyConfig.getBoolean(CoordinatorOptions.METADATA_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.GOVERNANCE_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.RESILIENCE_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.STREAMING_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.TENANT_ENABLED)).isFalse();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.INDEXING_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.OPERATIONS_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.OBSERVABILITY_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(CoordinatorOptions.SECURITY_ENABLED)).isFalse();
    }
}
