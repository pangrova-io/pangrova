package org.pangrova.client.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.transport.security.ClientAuthentication;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual driver configuration mapping; does not claim a successful LDAP bind or query. */
class JdbcBasicAuthenticationConfigTest {
    @AfterEach void closeSharedClient() { PangrovaDriver.shutdownSharedClient(); }

    @Test
    void standardDriverUserAndPasswordBecomeTheSoleUtf8BasicHeader() throws Exception {
        var props = new Properties();
        props.setProperty("user", "读者");
        props.setProperty("password", "密碼:a&b");
        try (var connection = (PangrovaConnection) new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", props)) {
            var auth = connection.restClient().config().authentication();
            assertThat(auth.scheme()).isEqualTo(ClientAuthentication.Scheme.BASIC);
            assertThat(auth.authorizationHeader()).isEqualTo(ClientAuthentication.basic("读者", "密碼:a&b").authorizationHeader());
            assertThat(connection.restClient().config().toString()).doesNotContain("读者", "密碼", "a&b");
        }
    }

    @Test
    void encodedSeparatorsAndUtf8AreDecodedOnlyAfterSplittingRawQueryComponents() throws Exception {
        String user = java.net.URLEncoder.encode("读者", StandardCharsets.UTF_8);
        String password = java.net.URLEncoder.encode("p:a&b=c+密", StandardCharsets.UTF_8);
        String url = "jdbc:pangrova://localhost:8080?user=" + user + "&password=" + password;
        var params = JdbcUrlParser.parse(url);
        assertThat(params.properties()).containsEntry("user", "读者").containsEntry("password", "p:a&b=c+密");
        assertThat(params.toString()).doesNotContain("p:a&b=c+密", password, "读者");
        try (var connection = (PangrovaConnection) new PangrovaDriver().connect(url, null)) {
            assertThat(connection.restClient().config().authentication().authorizationHeader())
                    .isEqualTo(ClientAuthentication.basic("读者", "p:a&b=c+密").authorizationHeader());
        }
    }

    @Test
    void conflictingOrIncompleteCredentialsAreRejectedWithoutConnecting() {
        for (String suffix : new String[]{"?token=bearer&user=reader&password=secret", "?user=reader", "?password=secret"}) {
            assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080" + suffix, null))
                    .isInstanceOf(SQLException.class);
        }
    }
}
