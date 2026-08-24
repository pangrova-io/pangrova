package org.pangrova.coordinator.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Bootstrap-time environment checks that must run before the coordinator starts
 * serving traffic.
 *
 * <p>These fail fast on misconfiguration that would otherwise surface much later
 * as a confusing runtime error: an unreachable metadata store looks like a query
 * failure, and a loopback advertised host looks like workers silently ignoring
 * the coordinator.
 */
final class CoordinatorPreflightChecks {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorPreflightChecks.class);

    /** Seconds allowed for the JDBC driver's own validity probe. */
    private static final int VALIDATION_TIMEOUT_SECONDS = 5;

    private CoordinatorPreflightChecks() {
    }

    /**
     * Warns when the coordinator advertises a loopback address, which makes it
     * unreachable from any worker on another host.
     */
    static void validateAdvertisedHost(CoordinatorBootstrapConfig config) {
        if (config.transport().isLoopbackAdvertisedHost()) {
            LOG.warn("event=loopback_advertised_host host={}",
                    config.coordinatorTransportAdvertisedHost());
        }
    }

    /**
     * Verifies the metadata store is reachable, so a bad URL or credential fails
     * during bootstrap rather than on the first query.
     *
     * @throws IllegalStateException if a connection cannot be obtained or is invalid
     */
    static void validateJdbcConnection(DataSource metadataDataSource) {
        if (metadataDataSource == null) {
            LOG.info("event=jdbc_validation_skipped reason=no_datasource");
            return;
        }
        try (Connection conn = metadataDataSource.getConnection()) {
            if (!conn.isValid(VALIDATION_TIMEOUT_SECONDS)) {
                throw new IllegalStateException(
                        "JDBC connection validation failed: connection is not valid");
            }
            LOG.info("event=jdbc_connection_validated");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "JDBC connection validation failed during bootstrap. "
                            + "Check JDBC URL and credentials. error=" + e.getMessage(), e);
        }
    }
}
