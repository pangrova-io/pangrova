package org.pangrova.testing.e2e.security;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.LdapAuthenticator;
import org.pangrova.security.authn.BypassAuthenticator;
import org.pangrova.security.authn.OAuth2Authenticator;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pangrova.testing.e2e.security.E2eRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for security authentication and authorization flow.
 *
 * <p>Validates the full security pipeline: NoOp auth pass-through,
 * OAuth2 token validation (mock IdP), LDAP bind, rejected invalid token,
 * and expired token handling through real authenticator implementations.
 *
 * <p>Validates: Requirements R12.1
 */
@DisplayName("Security Auth Flow E2E — AuthN/AuthZ Pipeline")
class SecurityAuthFlowE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static final RowLevelSecurityFilter NO_RLS =
            new RowLevelSecurityFilter(new EmptyRowPolicyStore());
    private static final ColumnMaskingProcessor NO_MASK =
            new ColumnMaskingProcessor(new EmptyColumnMaskStore());

    // Mock IdP inline HTTP server for OAuth2 token validation
    private static HttpServer mockIdpServer;
    private static int mockIdpPort;
    private static KeyPair rsaKeyPair;

    // TestContainers OpenLDAP for real LDAP bind tests
    @SuppressWarnings("resource")
    private static GenericContainer<?> openLdapContainer;

    private String testTable;

    @BeforeAll
    static void startMockIdpAndLdap() throws Exception {
        isDockerReady();
        // Generate RSA key pair for JWT signing in mock IdP
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        rsaKeyPair = keyGen.generateKeyPair();

        // Start inline HTTP server as mock OIDC IdP
        mockIdpServer = HttpServer.create(new InetSocketAddress(0), 0);
        mockIdpPort = mockIdpServer.getAddress().getPort();
        String issuerUri = "http://localhost:" + mockIdpPort;

        // OIDC discovery endpoint
        mockIdpServer.createContext("/.well-known/openid-configuration", exchange -> {
            String body = "{\"issuer\":\"" + issuerUri + "\","
                    + "\"jwks_uri\":\"" + issuerUri + "/jwks\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });

        // JWKS endpoint serving the RSA public key
        RSAPublicKey pub = (RSAPublicKey) rsaKeyPair.getPublic();
        String n = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(pub.getModulus().toByteArray());
        String e = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(pub.getPublicExponent().toByteArray());
        String jwksBody = "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"test-key-1\","
                + "\"n\":\"" + n + "\",\"e\":\"" + e + "\",\"alg\":\"RS256\",\"use\":\"sig\"}]}";

        mockIdpServer.createContext("/jwks", exchange -> {
            byte[] bytes = jwksBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });

        mockIdpServer.start();

        // Start OpenLDAP TestContainer
        openLdapContainer = new GenericContainer<>(DockerImageName.parse("osixia/openldap:latest"))
                .withNetwork(sharedNetwork)
                .withExposedPorts(389)
                .withEnv("LDAP_ORGANISATION", "Pangrova")
                .withEnv("LDAP_DOMAIN", "pangrova.org")
                .withEnv("LDAP_ADMIN_PASSWORD", "admin_pass")
                .withEnv("LDAP_BASE_DN", "dc=pangrova,dc=org");
        openLdapContainer.start();
    }

    @AfterAll
    static void stopMockIdpAndLdap() {
        if (mockIdpServer != null) {
            mockIdpServer.stop(0);
            mockIdpServer = null;
        }
        if (openLdapContainer != null) {
            openLdapContainer.stop();
            openLdapContainer = null;
        }
    }

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupSecurityTestTable() {
        String suffix = Long.toHexString(System.nanoTime());
        testTable = "sec_test_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, testTable,
                "id BIGINT, name VARCHAR, salary BIGINT, department VARCHAR");
        insertSecurityTestData();
    }

    // -----------------------------------------------------------------------
    // NoOp Authentication Pass-Through
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("NoOp Authentication")
    class NoOpAuthentication {

        @Test
        @DisplayName("NoOp authenticator always returns authenticated identity")
        void noOpAuthenticator_alwaysAuthenticated() {
            BypassAuthenticator authenticator = new BypassAuthenticator();
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, null, null, Map.of(), null);

            AuthenticationResult result = authenticator.authenticate(request);

            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity()).isNotNull();
            assertThat(result.userIdentity().principal()).isEqualTo("anonymous");
        }

        @Test
        @DisplayName("NoOp authenticator supports any request")
        void noOpAuthenticator_supportsAnyRequest() {
            BypassAuthenticator authenticator = new BypassAuthenticator();
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, null, null, Map.of(), null);

            assertThat(authenticator.supports(request)).isTrue();
        }

        @Test
        @DisplayName("NoOp auth allows query execution without credentials")
        void noOpAuth_allowsQueryWithoutCredentials() {
            String sql = String.format(
                    "SELECT id, name, department FROM %s.%s.%s ORDER BY id",
                    CATALOG, SCHEMA, testTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(10);
            for (int i = 0; i < rows.size(); i++) {
                long id = ((Number) rows.get(i).getRaw("id")).longValue();
                assertThat(id).isEqualTo(i + 1L);
            }
        }
    }

    // -----------------------------------------------------------------------
    // OAuth2 Token Validation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("OAuth2 Token Validation")
    class OAuth2TokenValidation {

        @Test
        @DisplayName("Missing token fails OAuth2 authentication")
        void missingToken_failsAuthentication() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of("issuerUri", "https://auth.example.com"));

                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, null, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("Empty token fails OAuth2 authentication")
        void emptyToken_failsAuthentication() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of("issuerUri", "https://auth.example.com"));

                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, "", null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("OAuth2 authenticator only supports requests with token")
        void oauth2_supportsOnlyTokenRequests() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                AuthenticationRequest withToken = new AuthenticationRequest(
                        null, null, "some.jwt.token", null, Map.of(), null);
                AuthenticationRequest withoutToken = new AuthenticationRequest(
                        null, null, null, null, Map.of(), null);

                assertThat(authenticator.supports(withToken)).isTrue();
                assertThat(authenticator.supports(withoutToken)).isFalse();
            }
        }
    }

    // -----------------------------------------------------------------------
    // OAuth2 Mock IdP — real OIDC discovery + JWKS + signed JWT
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("OAuth2 Mock IdP Validation")
    class OAuth2MockIdpValidation {

        @Test
        @DisplayName("Valid signed JWT from mock IdP authenticates successfully")
        void validSignedJwt_authenticatesViaMockIdp() throws Exception {
            String issuerUri = "http://localhost:" + mockIdpPort;
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", issuerUri,
                        "requireSignatureVerification", "true"));

                String jwt = buildSignedJwt(issuerUri, "test-user", 3600);
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, jwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isTrue();
                assertThat(result.userIdentity()).isNotNull();
                assertThat(result.userIdentity().principal()).isEqualTo("test-user");
            }
        }

        @Test
        @DisplayName("JWT with wrong issuer is rejected by mock IdP authenticator")
        void wrongIssuer_rejectedByMockIdp() throws Exception {
            String issuerUri = "http://localhost:" + mockIdpPort;
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", issuerUri,
                        "requireSignatureVerification", "false"));

                String jwt = buildUnsignedJwt("https://wrong-issuer.example.com", "hacker", 3600);
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, jwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("JWT with groups claim populates user groups from mock IdP")
        void jwtWithGroups_populatesUserGroups() throws Exception {
            String issuerUri = "http://localhost:" + mockIdpPort;
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", issuerUri,
                        "requireSignatureVerification", "false"));

                String jwt = buildUnsignedJwtWithGroups(issuerUri, "group-user", 3600,
                        "engineers", "admins");
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, jwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isTrue();
                assertThat(result.userIdentity().groups())
                        .containsExactlyInAnyOrder("engineers", "admins");
            }
        }
    }

    // -----------------------------------------------------------------------
    // LDAP Authentication — TestContainers OpenLDAP
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("LDAP Authentication")
    class LdapAuthentication {

        @Test
        @DisplayName("LDAP admin bind succeeds against TestContainers OpenLDAP")
        void ldapAdminBind_succeedsAgainstOpenLdap() {
            LdapAuthenticator authenticator = new LdapAuthenticator();
            String ldapUrl = "ldap://localhost:" + openLdapContainer.getMappedPort(389);
            authenticator.init(Map.of(
                    "ldapUrl", ldapUrl,
                    "baseDn", "dc=pangrova,dc=org",
                    "userDnPattern", "cn={0},dc=pangrova,dc=org",
                    "connectTimeoutMs", "5000",
                    "readTimeoutMs", "5000"));

            AuthenticationRequest request = new AuthenticationRequest(
                    "admin", "admin_pass".toCharArray(), null, null, Map.of(), "127.0.0.1");

            AuthenticationResult result = authenticator.authenticate(request);
            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity()).isNotNull();
            assertThat(result.userIdentity().principal()).isEqualTo("admin");
            authenticator.close();
        }

        @Test
        @DisplayName("LDAP bind with wrong password fails against TestContainers OpenLDAP")
        void ldapWrongPassword_failsAgainstOpenLdap() {
            LdapAuthenticator authenticator = new LdapAuthenticator();
            String ldapUrl = "ldap://localhost:" + openLdapContainer.getMappedPort(389);
            authenticator.init(Map.of(
                    "ldapUrl", ldapUrl,
                    "baseDn", "dc=pangrova,dc=org",
                    "userDnPattern", "cn={0},dc=pangrova,dc=org",
                    "connectTimeoutMs", "5000",
                    "readTimeoutMs", "5000"));

            AuthenticationRequest request = new AuthenticationRequest(
                    "admin", "wrong_password".toCharArray(), null, null, Map.of(), "127.0.0.1");

            AuthenticationResult result = authenticator.authenticate(request);
            assertThat(result.success()).isFalse();
            authenticator.close();
        }

        @Test
        @DisplayName("LDAP authenticator supports username+password requests only")
        void ldapAuthenticator_supportsUsernamePasswordOnly() {
            LdapAuthenticator authenticator = new LdapAuthenticator();

            AuthenticationRequest withCreds = new AuthenticationRequest(
                    "user", "pass".toCharArray(), null, null, Map.of(), null);
            AuthenticationRequest withToken = new AuthenticationRequest(
                    "user", "pass".toCharArray(), "token", null, Map.of(), null);
            AuthenticationRequest noPassword = new AuthenticationRequest(
                    "user", null, null, null, Map.of(), null);

            assertThat(authenticator.supports(withCreds)).isTrue();
            assertThat(authenticator.supports(withToken)).isFalse();
            assertThat(authenticator.supports(noPassword)).isFalse();
            authenticator.close();
        }

        @Test
        @DisplayName("LDAP health check passes with valid OpenLDAP connection")
        void ldapHealthCheck_passesWithValidConnection() {
            LdapAuthenticator authenticator = new LdapAuthenticator();
            String ldapUrl = "ldap://localhost:" + openLdapContainer.getMappedPort(389);
            authenticator.init(Map.of(
                    "ldapUrl", ldapUrl,
                    "baseDn", "dc=pangrova,dc=org",
                    "userDnPattern", "cn={0},dc=pangrova,dc=org",
                    "connectTimeoutMs", "5000",
                    "readTimeoutMs", "5000"));

            assertThat(authenticator.isHealthy()).isTrue();
            authenticator.close();
        }

        @Test
        @DisplayName("Query execution works with default auth (LDAP not blocking)")
        void queryExecution_worksWithDefaultAuth() {
            String sql = String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.%s",
                    CATALOG, SCHEMA, testTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(10L);
        }
    }

    // -----------------------------------------------------------------------
    // Invalid Token Rejection
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Invalid Token Rejection")
    class InvalidTokenRejection {

        @Test
        @DisplayName("Malformed JWT token is rejected by OAuth2 authenticator")
        void malformedToken_rejected() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of("issuerUri", "https://auth.example.com"));

                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, "not-a-jwt", null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("User without grant is denied access via RBAC")
        void userWithoutGrant_deniedAccess() {
            UserIdentity unauthorized = new UserIdentity(
                    "intruder", Set.of("unknown_group"), Map.of(), Instant.now());
            AccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analysts", "analysts"))),
                NO_RLS,
                NO_MASK);

            assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(unauthorized, CATALOG, SCHEMA, testTable))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        @DisplayName("Granted user passes, ungrantable privilege fails")
        void grantedUser_passes_ungrantedFails() {
            UserIdentity analyst = new UserIdentity(
                    "analyst", Set.of("analysts"), Map.of(), Instant.now());
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, testTable, null));
            AccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analysts", "analysts"))),
                NO_RLS,
                NO_MASK);

            // SELECT granted — should pass
            ac.checkCanSelectFromTable(analyst, CATALOG, SCHEMA, testTable);

            // INSERT not granted — should fail
            assertThatThrownBy(() ->
                    ac.checkCanInsertIntoTable(analyst, CATALOG, SCHEMA, testTable))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Expired Token Handling
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Expired Token Handling")
    class ExpiredTokenHandling {

        @Test
        @DisplayName("Expired JWT token is rejected by OAuth2 authenticator")
        void expiredToken_rejected() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", "https://auth.example.com",
                        "requireSignatureVerification", "false"));

                String expiredJwt = buildExpiredJwtToken();
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, expiredJwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("Token with missing subject claim is rejected")
        void missingSubject_rejected() {
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", "https://auth.example.com",
                        "requireSignatureVerification", "false"));

                String noSubJwt = buildJwtWithoutSubject();
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, noSubJwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("Expired signed JWT from mock IdP is rejected")
        void expiredSignedJwt_rejectedByMockIdp() throws Exception {
            String issuerUri = "http://localhost:" + mockIdpPort;
            try (OAuth2Authenticator authenticator = new OAuth2Authenticator()) {
                authenticator.init(Map.of(
                        "issuerUri", issuerUri,
                        "requireSignatureVerification", "true"));

                // Build a signed JWT that expired 1 hour ago
                String expiredJwt = buildSignedJwt(issuerUri, "expired-user", -3600);
                AuthenticationRequest request = new AuthenticationRequest(
                        null, null, expiredJwt, null, Map.of(), "127.0.0.1");

                AuthenticationResult result = authenticator.authenticate(request);
                assertThat(result.success()).isFalse();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Data Path Verification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Data Path Verification")
    class DataPathVerification {

        @Test
        @DisplayName("Aggregation query on authorized table returns correct result")
        void aggregationOnAuthorizedTable() {
            String sql = String.format(
                    "SELECT department, COUNT(*) AS cnt, SUM(salary) AS total_salary "
                    + "FROM %s.%s.%s GROUP BY department ORDER BY department",
                    CATALOG, SCHEMA, testTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalCount = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();
            assertThat(totalCount).isEqualTo(10L);
        }

        @Test
        @DisplayName("WHERE clause filters rows correctly")
        void whereClauseFiltersRows() {
            String sql = String.format(
                    "SELECT id, name FROM %s.%s.%s "
                    + "WHERE department = 'engineering' ORDER BY id",
                    CATALOG, SCHEMA, testTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            assertThat(rows.size()).isLessThan(10);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void insertSecurityTestData() {
        String[] departments = {"engineering", "sales", "hr", "finance"};
        for (int i = 1; i <= 10; i++) {
            String name = "user_" + i;
            long salary = 50000 + (i * 5000);
            String dept = departments[(i - 1) % departments.length];
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name, salary, department) "
                    + "VALUES (%d, '%s', %d, '%s')",
                    CATALOG, SCHEMA, testTable, i, name, salary, dept));
        }
    }

    /** Builds a structurally valid but expired JWT (unsigned, exp=0). */
    private static String buildExpiredJwtToken() {
        String header = encodeJson("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = encodeJson(
                "{\"sub\":\"test\",\"iss\":\"https://auth.example.com\",\"exp\":0}");
        return header + "." + payload + ".";
    }

    /** Builds a JWT without a subject claim. */
    private static String buildJwtWithoutSubject() {
        String header = encodeJson("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        long futureExp = Instant.now().plusSeconds(3600).getEpochSecond();
        String payload = encodeJson(
                "{\"iss\":\"https://auth.example.com\",\"exp\":" + futureExp + "}");
        return header + "." + payload + ".";
    }

    private static String encodeJson(String json) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Builds a signed JWT using the RSA key pair from the mock IdP.
     *
     * @param issuer  the issuer URI (must match mock IdP)
     * @param subject the subject claim
     * @param ttlSeconds seconds until expiration (negative = already expired)
     */
    private static String buildSignedJwt(String issuer, String subject, long ttlSeconds)
            throws Exception {
        String header = encodeJson("{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"test-key-1\"}");
        long exp = Instant.now().plusSeconds(ttlSeconds).getEpochSecond();
        String payload = encodeJson(
                "{\"sub\":\"" + subject + "\",\"iss\":\"" + issuer + "\",\"exp\":" + exp + "}");
        String signingInput = header + "." + payload;
        java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
        sig.initSign(rsaKeyPair.getPrivate());
        sig.update(signingInput.getBytes(StandardCharsets.UTF_8));
        byte[] signature = sig.sign();
        String encodedSig = Base64.getUrlEncoder().withoutPadding().encodeToString(signature);
        return signingInput + "." + encodedSig;
    }

    /** Builds an unsigned JWT with the given issuer and subject. */
    private static String buildUnsignedJwt(String issuer, String subject, long ttlSeconds) {
        String header = encodeJson("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        long exp = Instant.now().plusSeconds(ttlSeconds).getEpochSecond();
        String payload = encodeJson(
                "{\"sub\":\"" + subject + "\",\"iss\":\"" + issuer + "\",\"exp\":" + exp + "}");
        return header + "." + payload + ".";
    }

    /** Builds an unsigned JWT with groups claim. */
    private static String buildUnsignedJwtWithGroups(
            String issuer, String subject, long ttlSeconds, String... groups) {
        String header = encodeJson("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        long exp = Instant.now().plusSeconds(ttlSeconds).getEpochSecond();
        StringBuilder groupsJson = new StringBuilder("[");
        for (int i = 0; i < groups.length; i++) {
            if (i > 0) groupsJson.append(",");
            groupsJson.append("\"").append(groups[i]).append("\"");
        }
        groupsJson.append("]");
        String payload = encodeJson(
                "{\"sub\":\"" + subject + "\",\"iss\":\"" + issuer
                        + "\",\"exp\":" + exp + ",\"groups\":" + groupsJson + "}");
        return header + "." + payload + ".";
    }
}
