package org.pangrova.persistence.jdbc.security.support;

import org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate;

import org.pangrova.persistence.jdbc.security.codec.PolicyRoleCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for the security/governance policy stores persisted on the shared SQL state (SI-4).
 *
 * <p>Uses the supplied data source and owns only transactions it opens itself.
 * Applies the existing bundled security schema without changing its SQL bytes.
 */
public abstract class JdbcPolicyStoreSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPolicyStoreSupport.class);

    private static final String SCHEMA_RESOURCE =
            "org/pangrova/persistence/jdbc/security/support/security-policy-schema.sql";

    /** Shared SQL data source. Never null. */
    protected final DataSource dataSource;

    protected JdbcPolicyStoreSupport(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    /**
     * Applies the bundled policy schema DDL (idempotent {@code CREATE TABLE IF NOT EXISTS}). Safe to
     * call repeatedly at bootstrap. Index-creation statements are tolerated as already-existing.
     */
    public final void initSchema() {
        List<String> statements = loadSchemaStatements();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(conn);
            for (String ddl : statements) {
                var savepoint = conn.setSavepoint();
                try {
                    stmt.execute(ddl);
                } catch (SQLException e) {
                    // CREATE INDEX may lack IF NOT EXISTS; tolerate "already exists".
                    if (isAlreadyExists(e)) {
                        conn.rollback(savepoint);
                        LOG.debug("event=policy_schema_stmt_skipped reason=already_exists");
                    } else {
                        throw e;
                    }
                }
            }
            AuthorizationTransactionGate.initialize(conn);
            conn.commit();
            LOG.info("event=policy_schema_initialized statements={}", statements.size());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize security policy schema", e);
        }
    }

    private static boolean isAlreadyExists(SQLException e) {
        // Match PostgreSQL "already exists" errors from CREATE INDEX.
        // Must NOT match "does not exist" errors (wrong type, missing relation, etc.).
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("already exists") || lower.contains("relation already");
    }

    static List<String> loadSchemaStatements() {
        ClassLoader cl = JdbcPolicyStoreSupport.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Schema resource not found: " + SCHEMA_RESOURCE);
            }
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                        continue;
                    }
                    sb.append(line).append('\n');
                }
            }
            List<String> statements = new ArrayList<>();
            for (String raw : sb.toString().split(";")) {
                String ddl = raw.trim();
                if (!ddl.isEmpty()) {
                    statements.add(ddl);
                }
            }
            return statements;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading schema resource: " + SCHEMA_RESOURCE, e);
        }
    }

    /** Rolls back an owned transaction while retaining the original failure. */
    protected static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    protected static String encodeRoles(List<String> roles) {
        return PolicyRoleCodec.encodeRoles(roles);
    }

    protected static List<String> decodeRoles(String encoded) {
        return PolicyRoleCodec.decodeRoles(encoded);
    }
}
