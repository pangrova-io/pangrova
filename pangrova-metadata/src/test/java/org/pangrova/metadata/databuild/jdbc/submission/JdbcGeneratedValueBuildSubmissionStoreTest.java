package org.pangrova.metadata.databuild.jdbc.submission;

import org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** The business transaction adapter must preserve failures and release its owned connection. */
class JdbcGeneratedValueBuildSubmissionStoreTest {
    @Test
    void authorityAcquisitionFailureRollsBackBeforeClosingWithoutMaskingTheCause() {
        for (Throwable original : List.of(new SQLException("database failure"),
                new IllegalStateException("authority failure"), new AssertionError("driver error"))) {
            for (boolean rollbackFails : List.of(false, true)) {
                List<String> calls = new ArrayList<>();
                SQLException rollbackFailure = new SQLException("rollback failure");
                Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                            calls.add(method.getName());
                            return switch (method.getName()) {
                                case "setAutoCommit", "close" -> null;
                                case "getAutoCommit" -> false;
                                case "createStatement" -> throw original;
                                case "rollback" -> {
                                    if (rollbackFails) throw rollbackFailure;
                                    yield null;
                                }
                                default -> throw new AssertionError("Unexpected connection method: " + method);
                            };
                        });
                DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                            assertThat(method.getName()).isEqualTo("getConnection");
                            calls.add("getConnection");
                            return connection;
                        });
                Throwable actual = catchThrowable(() -> DataBuildAuthorityFixtures.artifacts().generatedSubmissions(source)
                        .submitCommittedBuilds(null, List.of()));
                if (original instanceof SQLException) {
                    assertThat(actual).isInstanceOf(MetastoreException.class).hasCause(original);
                } else {
                    assertThat(actual).isSameAs(original);
                }
                assertThat(calls).containsExactly("getConnection", "setAutoCommit", "getAutoCommit",
                        "createStatement", "rollback", "close");
                if (rollbackFails) assertThat(original.getSuppressed()).containsExactly(rollbackFailure);
                else assertThat(original.getSuppressed()).isEmpty();
            }
        }
    }
}
