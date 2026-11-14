package org.pangrova.client.http.authentication;

import org.pangrova.client.http.PangrovaRestClient;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Credential encoding and immutable configuration semantics, not a successful authorization. */
class ClientAuthenticationTest {
    @Test
    void basicEncodingRetainsUtf8AndPasswordColons() {
        var auth = ClientAuthentication.basic("读者😀", "密碼:a&b");
        assertThat(auth.scheme()).isEqualTo(ClientAuthentication.Scheme.BASIC);
        assertThat(new String(Base64.getDecoder().decode(auth.authorizationHeader().substring(6)),
                StandardCharsets.UTF_8)).isEqualTo("读者😀:密碼:a&b");
        assertThat(auth.toString()).doesNotContain("读者", "密碼", "a&b", auth.authorizationHeader());
        assertThat(ClientAuthentication.basic("reader", "").authorizationHeader()).isEqualTo("Basic cmVhZGVyOg==");
    }

    @Test
    void invalidCredentialsNeverReachHttpHeaders() {
        for (String user : List.of("", " ", "a:b", "a\r\nb", "a\0b")) {
            assertThatThrownBy(() -> ClientAuthentication.basic(user, "password"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> ClientAuthentication.basic("reader", "a\nb"))
                .isInstanceOf(IllegalArgumentException.class);
        for (String token : List.of("", " ", "one two", "one\r\nnext", "secret😀")) {
            assertThatThrownBy(() -> ClientAuthentication.bearer(token)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void bearerConvenienceAndTypedBasicShareOneMutuallyExclusiveAuthority() {
        var builder = PangrovaRestClient.builder().bearerToken("secret-token");
        assertThatThrownBy(() -> builder.authentication(ClientAuthentication.basic("reader", "secret")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already");
        try (var client = builder.build()) {
            assertThat(client.config().authentication().authorizationHeader()).isEqualTo("Bearer secret-token");
            assertThat(client.config().toString()).doesNotContain("secret-token", "Bearer secret");
        }
    }
}
