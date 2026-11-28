package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.persistence.jdbc.migration.SchemaMigrationRunner;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.auth.ldap.LdapAuthenticator;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

/** Owns real directory authentication and isolated migrated PostgreSQL state. */
final class ProcessSecurityAuthority implements AutoCloseable {
    static final String USER = "spec_user";
    static final String PASSWORD = "spéc:process-password";
    static final String TENANT = "spec-process-tenant";
    private final String schema = "spec_" + UUID.randomUUID().toString().replace("-", "");
    private final boolean isolatedDatabase;
    private final PGSimpleDataSource dataSource = new PGSimpleDataSource();
    private final LinkedHashSet<String> catalogs = new LinkedHashSet<>(List.of("spec_catalog"));
    private final GenericContainer<?> ldap;
    private boolean storageCreated;

    ProcessSecurityAuthority(Network network) {
        this(network, false);
    }

    ProcessSecurityAuthority(Network network, boolean isolatedDatabase) {
        this.isolatedDatabase = isolatedDatabase;
        ldap = new GenericContainer<>(DockerImageName.parse("osixia/openldap:1.5.0"))
                .withNetwork(network).withNetworkAliases("spec-directory").withExposedPorts(389)
                .withEnv("LDAP_ORGANISATION", "Pangrova")
                .withEnv("LDAP_DOMAIN", "pangrova.org").withEnv("LDAP_ADMIN_PASSWORD", "directory-admin")
                .withEnv("LDAP_TLS", "false")
                .withCopyToContainer(Transferable.of(ldif().getBytes(StandardCharsets.UTF_8)),
                        "/container/service/slapd/assets/config/bootstrap/ldif/custom/spec-user.ldif")
                .withCommand("--copy-service")
                // The image opens port 389 during LDIF bootstrap and closes it before serving.
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*First start is done[.]{3}.*\\s", 1))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(120)));
    }

    void start() throws Exception {
        try {
            initializeDatabase();
            ldap.start();
            verifyDirectory();
        } catch (Exception | Error startupFailure) {
            try { close(); } catch (Exception cleanupFailure) { startupFailure.addSuppressed(cleanupFailure); }
            throw startupFailure;
        }
    }

    PGSimpleDataSource dataSource() { return dataSource; }

    void allowCatalog(String catalog) {
        if (storageCreated) throw new IllegalStateException("Process permissions are already initialized");
        catalogs.add(catalog);
    }
    String hostLdapUrl() {
        return "ldap://" + ldap.getHost() + ":" + ldap.getMappedPort(389);
    }
    String hostJdbcUrl() {
        String url = SharedTestContainers.POSTGRES.getJdbcUrl();
        if (isolatedDatabase) return url.replaceFirst("/[^/?]+(?=\\?|$)", "/" + schema);
        return url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema;
    }
    String processJdbcUrl() {
        return "jdbc:postgresql://host.testcontainers.internal:" + SharedTestContainers.POSTGRES.getMappedPort(5432)
                + (isolatedDatabase ? "/" + schema : "/pangrova_meta?currentSchema=" + schema);
    }
    int postgresPort() { return SharedTestContainers.POSTGRES.getMappedPort(5432); }

    private void initializeDatabase() throws Exception {
        var postgres = SharedTestContainers.POSTGRES;
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            statement.execute((isolatedDatabase ? "CREATE DATABASE " : "CREATE SCHEMA ") + schema);
            storageCreated = true;
        }
        dataSource.setURL(hostJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SchemaMigrationRunner.migrate(dataSource);
        initializePermissions();
    }

    private void initializePermissions() {
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of("spec_process_operator"));
        roles.grantRoleToUser(UserRoleAssignment.of(USER, "spec_process_operator"));
        var grants = new JdbcGrantStore(dataSource);
        for (String catalog : catalogs) {
            grants.addGrant(PrivilegeGrant.of("spec_process_operator", Privilege.ALL, "CATALOG", catalog, "*", "*", null));
        }
        var tenants = new JdbcTenantStore(dataSource);

        tenants.createTenant(new Tenant(TENANT, "Process speculation", TenantStatus.ACTIVE,
                new ResourceQuota(1L << 30, 2, 2, 300_000, 4L << 30, 100, 40L << 30, 4L << 30),
                List.copyOf(catalogs), List.of("default"), null, Map.of(), Instant.now(), USER));
        tenants.addUserToTenant(TENANT, USER, TenantRole.OWNER);
    }

    private void verifyDirectory() {
        try (var authenticator = new LdapAuthenticator()) {
            authenticator.init(Map.of("ldapUrl", hostLdapUrl(),
                    "baseDn", "dc=pangrova,dc=org", "userDnPattern", "uid={0},dc=pangrova,dc=org"));
            await("actual directory user bind").pollDelay(Duration.ZERO).atMost(Duration.ofSeconds(30)).until(() -> {
                try (var request = new AuthenticationRequest(USER, PASSWORD.toCharArray(), null, null, Map.of(), "127.0.0.1")) {
                    return authenticator.authenticate(request).success();
                }
            });
        }
    }

    private static String ldif() {
        return "dn: uid=" + USER + ",dc=pangrova,dc=org\nobjectClass: inetOrgPerson\n"
                + "cn: Process User\nsn: User\nuid: " + USER + "\nuserPassword:: "
                + java.util.Base64.getEncoder().encodeToString(PASSWORD.getBytes(StandardCharsets.UTF_8)) + "\n";
    }

    @Override public void close() throws Exception {
        try (AutoCloseable directory = ldap::stop) {
            if (storageCreated) {
                var postgres = SharedTestContainers.POSTGRES;
                try (var connection = DriverManager.getConnection(
                        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                     var statement = connection.createStatement()) {
                    String drop = isolatedDatabase ? "DROP DATABASE " + schema : "DROP SCHEMA " + schema + " CASCADE";
                    statement.execute(drop);
                    storageCreated = false;
                }
            }
        }
    }
}
