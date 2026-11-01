package org.pangrova.security.store;

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
 * Applies the bundled security/governance policy schema (SI-4) to a shared SQL data source.
 *
 * <p>The schema is intentionally NOT Flyway-managed: security self-applies the bundled
 * {@code security-policy-schema.sql} (idempotent DDL) so this module keeps its
 * {@code security → spi, common} dependency and introduces no storage-backend coupling.
 * The same script serves production bootstrap (early, so the query baseline's
 * {@code storage_profile_revisions} foreign key resolves) and test containers.</p>
 *
 * <p>Authority-gate triggers and grant-guard functions are installed separately at runtime by
 * {@link AuthorizationTransactionGate#initialize(Connection)}; they are absent from the script.</p>
 */
public final class SecuritySchema {

    private static final Logger LOG = LoggerFactory.getLogger(SecuritySchema.class);

    static final String SCHEMA_RESOURCE =
            "org/pangrova/security/store/security-policy-schema.sql";

    private SecuritySchema() {
    }

    /**
     * Applies the bundled policy schema DDL (idempotent {@code CREATE TABLE IF NOT EXISTS}).
     * Safe to call repeatedly at bootstrap; already-existing objects are tolerated via
     * savepoint rollback. Does NOT install the runtime authority gate.
     *
     * @param dataSource the shared SQL data source; must not be null
     */
    public static void initialize(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        List<String> statements = loadSchemaStatements();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            for (String ddl : statements) {
                var savepoint = conn.setSavepoint();
                try {
                    stmt.execute(ddl);
                } catch (SQLException e) {
                    // Idempotent re-application: tolerate objects that already exist.
                    if (isAlreadyExists(e)) {
                        conn.rollback(savepoint);
                        LOG.debug("event=policy_schema_stmt_skipped reason=already_exists");
                    } else {
                        throw e;
                    }
                }
            }
            conn.commit();
            LOG.info("event=policy_schema_initialized statements={}", statements.size());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize security policy schema", e);
        }
    }

    private static boolean isAlreadyExists(SQLException e) {
        // Match PostgreSQL "already exists" errors from CREATE INDEX/CREATE TRIGGER/ADD CONSTRAINT.
        // Must NOT match "does not exist" errors (wrong type, missing relation, etc.).
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("already exists") || lower.contains("relation already");
    }

    static List<String> loadSchemaStatements() {
        ClassLoader cl = SecuritySchema.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Schema resource not found: " + SCHEMA_RESOURCE);
            }
            String script = readFully(in);
            return splitStatements(script);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading schema resource: " + SCHEMA_RESOURCE, e);
        }
    }

    private static String readFully(InputStream in) throws IOException {
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
        return sb.toString();
    }

    /**
     * Splits a SQL script on top-level {@code ;} only, ignoring semicolons inside single-quoted
     * string literals ({@code ''} escapes included) and dollar-quoted function bodies
     * ({@code $$} and {@code $tag$} forms).
     */
    static List<String> splitStatements(String script) {
        List<String> statements = new ArrayList<>();
        int length = script.length();
        int start = 0;
        String dollarTag = null;
        int i = 0;
        while (i < length) {
            char c = script.charAt(i);
            if (dollarTag != null) {
                if (script.startsWith(dollarTag, i)) {
                    i += dollarTag.length();
                    dollarTag = null;
                    continue;
                }
                i++;
                continue;
            }
            if (c == '\'') {
                i++;
                while (i < length) {
                    if (script.charAt(i) == '\'') {
                        if (i + 1 < length && script.charAt(i + 1) == '\'') {
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
                continue;
            }
            if (c == '$' && i + 1 < length && script.charAt(i + 1) == '$') {
                dollarTag = "$$";
                i += 2;
                continue;
            }
            if (c == '$') {
                int j = i + 1;
                while (j < length && (Character.isLetterOrDigit(script.charAt(j)) || script.charAt(j) == '_')) {
                    j++;
                }
                if (j < length && script.charAt(j) == '$' && j > i + 1) {
                    dollarTag = script.substring(i, j + 1);
                    i = j + 1;
                    continue;
                }
            }
            if (c == ';') {
                String stmt = script.substring(start, i).trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                start = i + 1;
            }
            i++;
        }
        String tail = script.substring(start).trim();
        if (!tail.isEmpty()) {
            statements.add(tail);
        }
        return statements;
    }
}
