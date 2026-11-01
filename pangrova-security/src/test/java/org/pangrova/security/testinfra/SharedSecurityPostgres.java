package org.pangrova.security.testinfra;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.pangrova.security.store.AuthorizationTransactionGate;

/**
 * One JVM-level PostgreSQL instance with an isolated schema per security test.
 *
 * <p>Storage-domain tests (file spaces, directory index, storage profiles,
 * tenant-backed file authority) bring their own clean-cut baseline instead of
 * the metadata migration chain: {@link #storageDomainDataSource(String)} applies
 * the bundled {@code db/security-storage-domain.sql} and then installs the
 * authority gates, so no test here depends on {@code pangrova-metadata}.</p>
 */
public final class SharedSecurityPostgres {
    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();
    private static final String STORAGE_DOMAIN_SCHEMA = "db/security-storage-domain.sql";

    private SharedSecurityPostgres() {}

    /** Returns a datasource whose objects cannot collide with another test's rows. */
    public static DataSource isolatedDataSource() throws SQLException {
        return isolatedSchema("test");
    }

    /**
     * Isolated schema with the full storage-domain baseline and installed
     * authority gates, ready for {@code JdbcFileSpaceRegistry},
     * {@code JdbcFileDirectoryIndex}, {@code JdbcStorageProfileRegistry} and
     * {@code JdbcTenantStore}-backed authority tests.
     */
    public static DataSource storageDomainDataSource(String prefix) throws SQLException {
        DataSource source = isolatedSchema(prefix);
        executeResource(source, STORAGE_DOMAIN_SCHEMA);
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.initialize(connection);
            connection.commit();
        }
        return source;
    }

    /**
     * Isolated schema carrying only the given classpath resources, in order,
     * without authority gates. Used by upgrade-boundary tests that need to
     * stage a legacy shape before applying the upgrade script.
     */
    public static DataSource schemaDataSource(String prefix, String... resources)
            throws SQLException {
        DataSource source = isolatedSchema(prefix);
        for (String resource : resources) {
            executeResource(source, resource);
        }
        return source;
    }

    /** Applies one additional classpath SQL resource to an existing isolated datasource. */
    public static void executeResource(DataSource source, String resource)
            throws SQLException {
        List<String> statements = loadStatements(resource);
        try (var connection = source.getConnection();
                var statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static DataSource isolatedSchema(String prefix) throws SQLException {
        String normalized = prefix.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]", "_");
        if (normalized.length() > 20) {
            normalized = normalized.substring(0, 20);
        }
        String schema = "security_" + normalized + "_"
                + UUID.randomUUID().toString().replace("-", "");
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        source.setCurrentSchema(schema);
        return source;
    }

    static List<String> loadStatements(String resource) {
        String sql;
        try (InputStream in = SharedSecurityPostgres.class.getClassLoader()
                .getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Schema resource not found: " + resource);
            }
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().startsWith("--")) {
                        sb.append(line).append('\n');
                    }
                }
            }
            sql = sb.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading schema resource: " + resource, e);
        }
        return splitStatements(sql);
    }

    /**
     * Splits SQL on semicolons outside string literals and dollar-quoted
     * bodies, so trigger functions keep their internal semicolons.
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inDollar = false;
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '$' && i + 1 < n && sql.charAt(i + 1) == '$') {
                inDollar = !inDollar;
                current.append("$$");
                i += 2;
                continue;
            }
            if (!inDollar && c == '\'') {
                current.append(c);
                i++;
                while (i < n) {
                    char q = sql.charAt(i);
                    if (q == '\'') {
                        if (i + 1 < n && sql.charAt(i + 1) == '\'') {
                            current.append("''");
                            i += 2;
                            continue;
                        }
                        current.append(q);
                        i++;
                        break;
                    }
                    current.append(q);
                    i++;
                }
                continue;
            }
            if (!inDollar && c == ';') {
                addStatement(statements, current);
                i++;
                continue;
            }
            current.append(c);
            i++;
        }
        addStatement(statements, current);
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder current) {
        String trimmed = current.toString().trim();
        if (!trimmed.isEmpty()) {
            statements.add(trimmed);
        }
        current.setLength(0);
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        container.start();
        return container;
    }
}
