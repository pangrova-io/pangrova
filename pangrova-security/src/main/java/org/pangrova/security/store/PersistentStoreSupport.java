package org.pangrova.security.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for the security/governance policy stores persisted on the shared SQL state (SI-4).
 *
 * <p>Holds the shared {@link DataSource} (obtained via {@code pangrova-common}'s state-store wiring, so
 * this module keeps its {@code security → spi, common} dependency and introduces no storage-backend
 * coupling) and applies the bundled {@code security-policy-schema.sql} DDL. Concrete stores
 * ({@code PersistentRowPolicyStore}, {@code PersistentColumnMaskStore}, {@code PersistentTenantStore})
 * extend this. Per naming rule C13 these are {@code Persistent*}, not {@code Jdbc*}, because
 * {@code pangrova-security} is a policy module rather than the owning persistence module.
 */
public abstract class PersistentStoreSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentStoreSupport.class);

    /** Shared SQL data source. Never null. */
    protected final DataSource dataSource;

    protected PersistentStoreSupport(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    /**
     * Applies the bundled policy schema DDL (idempotent {@code CREATE TABLE IF NOT EXISTS}). Safe to
     * call repeatedly at bootstrap. Index-creation statements are tolerated as already-existing.
     * Also installs the runtime authority gate (idempotent {@code CREATE OR REPLACE} objects).
     */
    public final void initSchema() {
        SecuritySchema.initialize(dataSource);
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(conn);
            AuthorizationTransactionGate.initialize(conn);
            conn.commit();
            LOG.info("event=authority_gate_initialized");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize authority gate", e);
        }
    }

    /** Joins a role list to the comma-separated form stored in {@code apply_to_roles}/{@code exempt_roles}. */
    protected static String encodeRoles(List<String> roles) {
        return roles == null || roles.isEmpty() ? "" : String.join(",", roles);
    }

    /** Parses the comma-separated stored role form back into a list ('' → empty list). */
    protected static List<String> decodeRoles(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return List.of();
        }
        return List.of(encoded.split(","));
    }
}
