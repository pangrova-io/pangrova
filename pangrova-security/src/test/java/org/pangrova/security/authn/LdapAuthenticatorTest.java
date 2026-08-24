package org.pangrova.security.authn;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.junit.jupiter.api.Test;

/**
 * Tests LdapAuthenticator logic: input validation, config modes, filter escaping,
 * and authentication flows using a testable subclass that overrides JNDI context creation.
 *
 * No mock frameworks — uses a minimal DirContext test double implemented inline.
 */
class LdapAuthenticatorTest {

    // --- Input validation tests ---

    @Test
    void rejectsMissingUsername() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            auth.init(Map.of());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("Username and password required");
        }
    }

    @Test
    void rejectsMissingPassword() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            auth.init(Map.of());
            AuthenticationRequest request = new AuthenticationRequest(
                    "user", null, null, null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("Username and password required");
        }
    }

    @Test
    void rejectsWhenNotInitialized() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            // Do NOT call init()
            AuthenticationRequest request = new AuthenticationRequest(
                    "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("not initialized");
        }
    }

    // --- supports() tests ---

    @Test
    void supportsUsernamePasswordWithoutToken() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            AuthenticationRequest ldapReq = new AuthenticationRequest(
                    "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
            assertThat(auth.supports(ldapReq)).isTrue();
        }
    }

    @Test
    void doesNotSupportTokenRequests() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            AuthenticationRequest tokenReq = new AuthenticationRequest(
                    "user", "pass".toCharArray(), "some-token", null, Map.of(), "127.0.0.1");
            assertThat(auth.supports(tokenReq)).isFalse();
        }
    }

    @Test
    void doesNotSupportMissingCredentials() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            AuthenticationRequest noUser = new AuthenticationRequest(
                    null, "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
            AuthenticationRequest noPass = new AuthenticationRequest(
                    "user", null, null, null, Map.of(), "127.0.0.1");
            assertThat(auth.supports(noUser)).isFalse();
            assertThat(auth.supports(noPass)).isFalse();
        }
    }

    // --- getName() ---

    @Test
    void nameIsLdap() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            assertThat(auth.getName()).isEqualTo("ldap");
        }
    }

    // --- Filter escaping tests ---

    @Test
    void escapesSpecialCharactersInFilterValues() {
        assertThat(LdapAuthenticator.escapeFilterValue("normal")).isEqualTo("normal");
        assertThat(LdapAuthenticator.escapeFilterValue("user*")).isEqualTo("user\\2a");
        assertThat(LdapAuthenticator.escapeFilterValue("(admin)")).isEqualTo("\\28admin\\29");
        assertThat(LdapAuthenticator.escapeFilterValue("back\\slash")).isEqualTo("back\\5cslash");
        assertThat(LdapAuthenticator.escapeFilterValue("null\0char")).isEqualTo("null\\00char");
        assertThat(LdapAuthenticator.escapeFilterValue(null)).isEqualTo("");
    }

    @Test
    void escapesCompoundInjectionAttempt() {
        // LDAP injection attempt: )(uid=*))(|(uid=*
        String malicious = ")(uid=*))(|(uid=*";
        String escaped = LdapAuthenticator.escapeFilterValue(malicious);
        assertThat(escaped).doesNotContain("(").doesNotContain(")").doesNotContain("*");
    }

    // --- Config mode detection ---

    @Test
    void configDetectsSearchThenBindMode() {
        var cfg = new LdapAuthenticator.LdapConfig(
                "ldap://localhost:389",
                "ou=users,dc=example,dc=com",
                "(uid={0})",
                "uid={0},ou=users,dc=example,dc=com",
                "cn=admin,dc=example,dc=com",
                "adminpass".toCharArray(),
                "ou=groups,dc=example,dc=com",
                "(member={0})",
                "cn", "5000", "5000");
        assertThat(cfg.isSearchThenBind()).isTrue();
    }

    @Test
    void configDetectsDirectBindMode() {
        var cfg = new LdapAuthenticator.LdapConfig(
                "ldap://localhost:389",
                "ou=users,dc=example,dc=com",
                "(uid={0})",
                "uid={0},ou=users,dc=example,dc=com",
                null, (char[]) null,
                "ou=groups,dc=example,dc=com",
                "(member={0})",
                "cn", "5000", "5000");
        assertThat(cfg.isSearchThenBind()).isFalse();
    }

    @Test
    void configDetectsDirectBindModeWithBlankBindDn() {
        var cfg = new LdapAuthenticator.LdapConfig(
                "ldap://localhost:389",
                "ou=users,dc=example,dc=com",
                "(uid={0})",
                "uid={0},ou=users,dc=example,dc=com",
                "  ", (char[]) null,
                "ou=groups,dc=example,dc=com",
                "(member={0})",
                "cn", "5000", "5000");
        assertThat(cfg.isSearchThenBind()).isFalse();
    }

    // --- Direct bind authentication flow ---

    @Test
    void directBind_successfulAuthentication() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.DIRECT_BIND_SUCCESS);
        auth.init(Map.of(
                "ldapUrl", "ldap://test:389",
                "userDnPattern", "uid={0},ou=users,dc=test,dc=com"
        ));

        AuthenticationRequest request = new AuthenticationRequest(
                "alice", "secret".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isTrue();
        assertThat(result.userIdentity()).isNotNull();
        assertThat(result.userIdentity().principal()).isEqualTo("alice");
        assertThat(result.userIdentity().groups()).containsExactlyInAnyOrder("developers", "admins");
        auth.close();
    }

    @Test
    void directBind_invalidCredentials() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.AUTH_FAILURE);
        auth.init(Map.of("ldapUrl", "ldap://test:389"));

        AuthenticationRequest request = new AuthenticationRequest(
                "alice", "wrong".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).isEqualTo("Invalid credentials");
        auth.close();
    }

    @Test
    void directBind_serverError() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.SERVER_ERROR);
        auth.init(Map.of("ldapUrl", "ldap://test:389"));

        AuthenticationRequest request = new AuthenticationRequest(
                "alice", "pass".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).isEqualTo("LDAP authentication error");
        auth.close();
    }

    // --- Search-then-bind authentication flow ---

    @Test
    void searchThenBind_successfulAuthentication() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.SEARCH_THEN_BIND_SUCCESS);
        auth.init(Map.of(
                "ldapUrl", "ldap://test:389",
                "bindDn", "cn=admin,dc=test,dc=com",
                "bindPassword", "adminpass",
                "baseDn", "ou=users,dc=test,dc=com",
                "userSearchFilter", "(uid={0})"
        ));

        AuthenticationRequest request = new AuthenticationRequest(
                "bob", "bobpass".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isTrue();
        assertThat(result.userIdentity().principal()).isEqualTo("bob");
        assertThat(result.userIdentity().groups()).contains("users");
        auth.close();
    }

    @Test
    void searchThenBind_userNotFound() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.SEARCH_USER_NOT_FOUND);
        auth.init(Map.of(
                "ldapUrl", "ldap://test:389",
                "bindDn", "cn=admin,dc=test,dc=com",
                "bindPassword", "adminpass",
                "baseDn", "ou=users,dc=test,dc=com",
                "userSearchFilter", "(uid={0})"
        ));

        AuthenticationRequest request = new AuthenticationRequest(
                "unknown", "pass".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).isEqualTo("Invalid credentials");
        auth.close();
    }

    // --- Health check ---

    @Test
    void healthCheck_falseWhenNotInitialized() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            assertThat(auth.isHealthy()).isFalse();
        }
    }

    // --- Error message does not leak backend details ---

    @Test
    void errorMessage_doesNotLeakLdapUrl() {
        TestableLdapAuthenticator auth = new TestableLdapAuthenticator(
                TestableLdapAuthenticator.Mode.SERVER_ERROR);
        auth.init(Map.of("ldapUrl", "ldap://internal-ldap.corp:389"));

        AuthenticationRequest request = new AuthenticationRequest(
                "alice", "pass".toCharArray(), null, null, Map.of(), "10.0.0.1");
        AuthenticationResult result = auth.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).doesNotContain("internal-ldap");
        assertThat(result.errorMessage()).doesNotContain("389");
        assertThat(result.errorMessage()).doesNotContain("ldap://");
        auth.close();
    }

    // --- Testable subclass that overrides JNDI context creation ---

    /**
     * Subclass that replaces real JNDI operations with deterministic test behavior.
     * This avoids mock frameworks while testing the authentication flow logic.
     */
    static class TestableLdapAuthenticator extends LdapAuthenticator {

        enum Mode {
            DIRECT_BIND_SUCCESS,
            SEARCH_THEN_BIND_SUCCESS,
            AUTH_FAILURE,
            SERVER_ERROR,
            SEARCH_USER_NOT_FOUND
        }

        private final Mode mode;

        TestableLdapAuthenticator(Mode mode) {
            this.mode = mode;
        }

        @Override
        DirContext createBindContext(LdapConfig cfg, String dn, char[] password) throws NamingException {
            return switch (mode) {
                case AUTH_FAILURE -> throw new AuthenticationException("Invalid credentials");
                case SERVER_ERROR -> throw new NamingException("Connection refused");
                case DIRECT_BIND_SUCCESS, SEARCH_THEN_BIND_SUCCESS, SEARCH_USER_NOT_FOUND ->
                        new StubDirContext(mode);
            };
        }

        @Override
        String resolveUserDn(LdapConfig cfg, String username) throws NamingException {
            if (cfg.isSearchThenBind()) {
                if (mode == Mode.SEARCH_USER_NOT_FOUND) {
                    throw new AuthenticationException("User not found in directory");
                }
                // Simulate finding the user DN via search
                return "uid=" + username + "," + cfg.baseDn();
            }
            return cfg.userDnPattern().replace("{0}", username);
        }

        @Override
        Set<String> searchGroups(DirContext ctx, LdapConfig cfg, String userDn) throws NamingException {
            return switch (mode) {
                case DIRECT_BIND_SUCCESS -> Set.of("developers", "admins");
                case SEARCH_THEN_BIND_SUCCESS -> Set.of("users");
                default -> Set.of();
            };
        }
    }

    /**
     * Minimal DirContext stub for test flow — only needs to exist without throwing.
     */
    static class StubDirContext extends InitialDirContext {
        private final TestableLdapAuthenticator.Mode mode;

        StubDirContext(TestableLdapAuthenticator.Mode mode) throws NamingException {
            super(true); // skip real JNDI initialization
            this.mode = mode;
        }

        @Override
        public void close() {
            // no-op for test
        }
    }
}
