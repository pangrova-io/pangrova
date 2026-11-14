package org.pangrova.persistence.generation.postgresql.job;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.generation.api.exception.GenerationStorageException;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class PostgresGenerationTransactionsTest {
    @Test
    void rollbackFailurePreservesOriginalFailureAndNeverEnablesAutoCommit() {
        for (Throwable original : List.of(new SQLException("write failed"),
                new IllegalStateException("validation failed"), new AssertionError("driver failed"))) {
            List<String> calls = new ArrayList<>();
            SQLException rollbackFailure = new SQLException("rollback failed");
            Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        calls.add(method.getName() + (args == null ? "" : List.of(args)));
                        return switch (method.getName()) {
                            case "setAutoCommit", "close" -> null;
                            case "getAutoCommit" -> true;
                            case "rollback" -> throw rollbackFailure;
                            default -> throw new AssertionError("Unexpected connection method: " + method);
                        };
                    });
            DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                        assertThat(method.getName()).isEqualTo("getConnection");
                        return connection;
                    });
            Throwable actual = catchThrowable(() -> new TransactionProbe(source).fail(original));
            if (original instanceof SQLException) {
                assertThat(actual).isInstanceOf(GenerationStorageException.class).hasCause(original);
                assertThat(((GenerationStorageException) actual).getErrorCode())
                        .isEqualTo(org.pangrova.common.enums.PangrovaErrorCode.METADATA_METASTORE_ERROR);
            } else {
                assertThat(actual).isSameAs(original);
            }
            assertThat(original.getSuppressed()).containsExactly(rollbackFailure);
            assertThat(calls).containsExactly("setAutoCommit[false]", "rollback", "close");
        }
    }

    private static final class TransactionProbe extends PostgresGenerationJobStoreMappings {
        private TransactionProbe(DataSource source) {
            super(source);
        }

        private void fail(Throwable failure) {
            inTransaction("fixture", connection -> {
                if (failure instanceof SQLException sql) throw sql;
                if (failure instanceof RuntimeException runtime) throw runtime;
                throw (Error) failure;
            });
        }
    }
}
