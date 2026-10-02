package org.pangrova.security.authn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Verifies authenticator behavior for OAuth2 and LDAP providers.
 * Tests cover token validation, credential handling, and supports() logic.
 */
class AuthenticationContractTest {

    @Test
    void oauth2_rejectsNullToken() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, null, null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("Token required");
        }
    }

    @Test
    void oauth2_rejectsBlankToken() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, "  ", null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
        }
    }

    @Test
    void oauth2_rejectsWhenIssuerNotConfigured() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            assertThatThrownBy(() -> auth.init(Map.of()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("issuerUri");
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, "some-token", null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("issuer not configured");
        }
    }

    @Test
    void ldap_rejectsMissingCredentials() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            auth.init(Map.of());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, null, null, Map.of(), "127.0.0.1");
            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("Username and password required");
        }
    }

    @Test
    void oauth2_supportsTokenRequests() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            AuthenticationRequest withToken = new AuthenticationRequest(
                    null, null, "tok", null, Map.of(), "127.0.0.1");
            AuthenticationRequest withoutToken = new AuthenticationRequest(
                    null, null, null, null, Map.of(), "127.0.0.1");
            assertThat(auth.supports(withToken)).isTrue();
            assertThat(auth.supports(withoutToken)).isFalse();
        }
    }

    @Test
    void ldap_supportsUsernamePasswordRequests() {
        try (LdapAuthenticator auth = new LdapAuthenticator()) {
            AuthenticationRequest ldapReq = new AuthenticationRequest(
                    "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
            AuthenticationRequest tokenReq = new AuthenticationRequest(
                    "user", "pass".toCharArray(), "tok", null, Map.of(), "127.0.0.1");
            assertThat(auth.supports(ldapReq)).isTrue();
            assertThat(auth.supports(tokenReq)).isFalse();
        }
    }
}
