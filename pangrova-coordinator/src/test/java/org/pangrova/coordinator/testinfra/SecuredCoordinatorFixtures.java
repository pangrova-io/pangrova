package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;

/** Supplies real shared PostgreSQL authority to fixtures that actually start secured coordinators. */
public final class SecuredCoordinatorFixtures {
    private SecuredCoordinatorFixtures() {}

    /** Preserves all configured features and enables the required persistent metadata authority. */
    public static CoordinatorBootstrapConfig withAuthority(CoordinatorBootstrapConfig config) {
        if (!config.securityEnabled() || config.metadataEnabled()) {
            return config;
        }
        var flags = config.featureFlags();
        var persistent = new CoordinatorBootstrapConfig.FeatureFlags(
                flags.observability(), flags.security(), true,
                flags.streaming(), flags.governance(), flags.resilience(), flags.tenant(), flags.operations(),
                flags.speculation(), flags.pullModeSplitAssignment(), flags.cacheWriteback(), flags.indexing(),
                flags.embedding(), flags.ddlLeaderRouting(), flags.shuffleMode(), flags.shuffleEndpoints());
        try {
            var source = (PGSimpleDataSource) SharedPostgresContainer.isolatedDataSource("secured_fixture");
            var jdbc = new CoordinatorBootstrapConfig.MetadataJdbcConfig(
                    source.getURL(), source.getUser(), source.getPassword());
            return new CoordinatorBootstrapConfig(config.core(), persistent, config.query(), config.transport(),
                    config.workerHealth(), jdbc, config.securityConfig(), config.observabilityBootstrapConfig());
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot initialize secured fixture authority", failure);
        }
    }
}
