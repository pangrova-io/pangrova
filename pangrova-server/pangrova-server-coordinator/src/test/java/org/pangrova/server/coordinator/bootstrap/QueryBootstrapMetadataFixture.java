package org.pangrova.server.coordinator.bootstrap;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.connector.api.metadata.SourceCatalogDeployment;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.persistence.catalog.postgresql.source.JdbcManagedSourceCatalog;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import javax.sql.DataSource;
import java.util.Optional;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Real catalog runtime required by durable query bootstrap, including published-generation reads. */
final class QueryBootstrapMetadataFixture implements AutoCloseable {
    private final PluginConnectorHandleFactory connectors;
    private final MetadataBootstrap bootstrap;

    QueryBootstrapMetadataFixture(DataSource dataSource) {
        connectors = new PluginConnectorHandleFactory(ConnectorFactory.class.getClassLoader(),
                new SecretResolverRegistry(), descriptor -> Optional.empty(),
                descriptor -> Optional.of(new JdbcManagedSourceCatalog(dataSource,
                        new SourceCatalogDeployment(descriptor.name(), descriptor.incarnationId(), descriptor.revision()),
                        ignored -> { throw new IllegalStateException("Native writes use the test's separate control plane"); },
                        new JdbcExecutionTransactionFence(), ModelAuthorityFixtures.models(),
                                CoordinatorTableWriteAuthority.create()::requireCurrent)));
        try {
            bootstrap = MetadataBootstrap.initialize(memoryTableStores(), new PostgresCatalogDeploymentRepository(dataSource), new PostgresTableProjectionRepository(dataSource), new PostgresMetadataProjectionTransactions(dataSource), new PostgresMetadataSyncRepository(dataSource), connectors);
        } catch (RuntimeException failure) {
            try {
                connectors.shutdown();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    MetadataBootstrap bootstrap() {
        return bootstrap;
    }

    @Override
    public void close() throws Exception {
        // Try-with-resources preserves the first close failure and suppresses later failures.
        try (AutoCloseable factory = connectors::shutdown;
             AutoCloseable runtime = bootstrap::shutdown) {
            // Closing the runtime precedes closing its borrowed connector classloaders.
        }
    }
}
