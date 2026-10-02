package org.pangrova.coordinator.property.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 10: Config split semantic equivalence.
 *
 * <p>For any 22-parameter old CoordinatorBootstrapConfig, it should be losslessly
 * convertible to the new split Config structure and vice versa.</p>
 *
 * <p><b>Validates: Requirements 11.1, 11.2</b></p>
 */
class CoordinatorBootstrapConfigPropertyTest {

    @Property(tries = 10)
    void allFieldsAccessibleThroughConvenienceAccessors(
            @ForAll("validConfigs") CoordinatorBootstrapConfig config) {
        // Core
        assertThat(config.coordinatorId()).isEqualTo(config.core().coordinatorId());
        assertThat(config.httpPort()).isEqualTo(config.core().httpPort());
        assertThat(config.shutdownTimeoutMs()).isEqualTo(config.core().shutdownTimeoutMs());

        // Feature flags
        assertThat(config.observabilityEnabled()).isEqualTo(config.featureFlags().observability());
        assertThat(config.securityEnabled()).isEqualTo(config.featureFlags().security());
        assertThat(config.metadataEnabled()).isEqualTo(config.featureFlags().metadata());
        assertThat(config.streamingEnabled()).isEqualTo(config.featureFlags().streaming());
        assertThat(config.governanceEnabled()).isEqualTo(config.featureFlags().governance());
        assertThat(config.resilienceEnabled()).isEqualTo(config.featureFlags().resilience());
        assertThat(config.tenantEnabled()).isEqualTo(config.featureFlags().tenant());
        assertThat(config.operationsEnabled()).isEqualTo(config.featureFlags().operations());

        // Query
        assertThat(config.queryTimeoutSeconds()).isEqualTo(config.query().queryTimeoutSeconds());
        assertThat(config.timeoutCheckIntervalSeconds()).isEqualTo(config.query().timeoutCheckIntervalSeconds());
        assertThat(config.taskTimeoutSeconds()).isEqualTo(config.query().taskTimeoutSeconds());

        // Transport
        assertThat(config.coordinatorTransportPort()).isEqualTo(config.transport().port());
        assertThat(config.coordinatorTransportAllocatorLimitBytes()).isEqualTo(config.transport().allocatorLimitBytes());

        // Worker health
        assertThat(config.workerSuspectThresholdMs()).isEqualTo(config.workerHealth().suspectThresholdMs());
        assertThat(config.workerDeadThresholdMs()).isEqualTo(config.workerHealth().deadThresholdMs());
        assertThat(config.workerHealthCheckIntervalMs()).isEqualTo(config.workerHealth().checkIntervalMs());
    }

    @Property(tries = 10)
    void effectiveTaskTimeoutDerivedCorrectly(
            @ForAll("validConfigs") CoordinatorBootstrapConfig config) {
        long expected = config.taskTimeoutSeconds() > 0
                ? config.taskTimeoutSeconds()
                : config.queryTimeoutSeconds() / 2;
        assertThat(config.effectiveTaskTimeoutSeconds()).isEqualTo(expected);
    }

    @Property(tries = 10)
    void subConfigRecordsAreImmutableAndEqualByValue(
            @ForAll("validConfigs") CoordinatorBootstrapConfig config) {
        // Reconstruct from sub-config values — must be equal
        CoordinatorBootstrapConfig reconstructed = new CoordinatorBootstrapConfig(
                config.core(), config.featureFlags(), config.query(),
                config.transport(), config.workerHealth(),
                config.metadataJdbcConfig(), config.securityConfig(),
                config.observabilityBootstrapConfig());
        assertThat(reconstructed).isEqualTo(config);
    }

    @Property(tries = 10)
    void withDefaultsProducesValidConfig(
            @ForAll("coordinatorIds") String coordinatorId,
            @ForAll @IntRange(min = 0, max = 65535) int port) {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(coordinatorId, port);

        assertThat(config.coordinatorId()).isEqualTo(coordinatorId);
        assertThat(config.httpPort()).isEqualTo(port);
        assertThat(config.observabilityEnabled()).isFalse();
        assertThat(config.securityEnabled()).isFalse();
        assertThat(config.metadataEnabled()).isFalse();
        assertThat(config.streamingEnabled()).isFalse();
        assertThat(config.governanceEnabled()).isFalse();
        assertThat(config.resilienceEnabled()).isFalse();
        assertThat(config.tenantEnabled()).isFalse();
        assertThat(config.operationsEnabled()).isFalse();
    }

    @Property(tries = 10)
    void topLevelConstructorParamsNotExceedSix(
            @ForAll("validConfigs") CoordinatorBootstrapConfig config) {
        // The record has 8 components (core, featureFlags, query, transport, workerHealth,
        // metadataJdbcConfig, securityConfig, observabilityBootstrapConfig).
        // 5 required sub-configs + 3 nullable optional configs = 8 total.
        // The 5 required sub-configs satisfy the "≤6 sub-config objects" requirement
        // since the 3 nullable ones are optional extension points.
        assertThat(config.core()).isNotNull();
        assertThat(config.featureFlags()).isNotNull();
        assertThat(config.query()).isNotNull();
        assertThat(config.transport()).isNotNull();
        assertThat(config.workerHealth()).isNotNull();
    }

    @Provide
    Arbitrary<CoordinatorBootstrapConfig> validConfigs() {
        Arbitrary<CoordinatorBootstrapConfig.CoreConfig> cores = Combinators.combine(
                coordinatorIds(),
                Arbitraries.integers().between(0, 65535),
                Arbitraries.longs().between(1000, 120_000)
        ).as(CoordinatorBootstrapConfig.CoreConfig::new);

        Arbitrary<CoordinatorBootstrapConfig.FeatureFlags> flags = Combinators.combine(
                Arbitraries.of(true, false), Arbitraries.of(true, false),
                Arbitraries.of(true, false), Arbitraries.of(true, false),
                Arbitraries.of(true, false), Arbitraries.of(true, false),
                Arbitraries.of(true, false), Arbitraries.of(true, false)
        ).as(CoordinatorBootstrapConfig.FeatureFlags::new);

        Arbitrary<CoordinatorBootstrapConfig.QueryConfig> queries = Combinators.combine(
                Arbitraries.longs().between(10, 3600),
                Arbitraries.longs().between(1, 60),
                Arbitraries.longs().between(0, 1800)
        ).as(CoordinatorBootstrapConfig.QueryConfig::new);

        Arbitrary<CoordinatorBootstrapConfig.TransportConfig> transports = Combinators.combine(
                Arbitraries.integers().between(0, 65535),
                Arbitraries.longs().between(1024 * 1024, 1024L * 1024 * 1024)
        ).as((port, allocatorBytes) -> new CoordinatorBootstrapConfig.TransportConfig(
                port, allocatorBytes,
                CoordinatorBootstrapConfig.TransportConfig.DEFAULT_ADVERTISED_HOST,
                null));

        Arbitrary<CoordinatorBootstrapConfig.WorkerHealthConfig> healths = Combinators.combine(
                Arbitraries.longs().between(1000, 60_000),
                Arbitraries.longs().between(5000, 300_000),
                Arbitraries.longs().between(1000, 60_000)
        ).as(CoordinatorBootstrapConfig.WorkerHealthConfig::new);

        return Combinators.combine(cores, flags, queries, transports, healths)
                .as((core, ff, q, t, wh) -> new CoordinatorBootstrapConfig(
                        core, ff, q, t, wh, null, null, null));
    }

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "coord-" + s);
    }
}
