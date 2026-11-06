package org.pangrova.persistence.catalog.postgresql.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Public JDBC fault injection around real failed outbox transactions. */
class PostgresCatalogCleanupFailureTest {
    private DataSource source;
    private CatalogDescriptor descriptor;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        descriptor = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE invalidation_log ADD CONSTRAINT reject_invalidation CHECK (cache_key <> 'catalog')");
        }
    }

    @Test
    void runtimeRollbackFailureDoesNotReplaceFailedOutboxWrite() throws Exception {
        verifyFailure(new IllegalStateException("rollback reply failed"));
    }

    @Test
    void rollbackErrorDoesNotReplaceFailedOutboxWrite() throws Exception {
        verifyFailure(new AssertionError("rollback driver failed"));
    }

    private void verifyFailure(Throwable rollbackFailure) throws Exception {
        AtomicInteger closed = new AtomicInteger();
        var provider = new PostgresCatalogDeploymentRepository(afterRollbackFailure(rollbackFailure, closed));
        Throwable failure = catchThrowable(() -> provider.saveCatalogDescriptor(descriptor));
        assertThat(failure).isInstanceOf(MetastoreException.class).hasCauseInstanceOf(SQLException.class);
        assertThat(failure.getCause()).hasMessageContaining("reject_invalidation");
        assertThat(failure.getCause().getSuppressed()).containsExactly(rollbackFailure);
        assertThat(closed).hasValue(1);
        assertThat(new PostgresCatalogDeploymentRepository(source).getCatalogDescriptor("catalog")).isEmpty();
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT (SELECT COUNT(*) FROM catalog_descriptor_revisions) "
                        + "+ (SELECT COUNT(*) FROM invalidation_log)")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isZero();
        }
    }

    private DataSource afterRollbackFailure(Throwable failure, AtomicInteger closed) {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object value = method.invoke(source, args);
                        if (!method.getName().equals("getConnection")) return value;
                        Connection delegate = (Connection) value;
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, call, parameters) -> {
                                    try {
                                        Object result = call.invoke(delegate, parameters);
                                        if (call.getName().equals("close")) closed.incrementAndGet();
                                        if (call.getName().equals("rollback")) throw failure;
                                        return result;
                                    } catch (InvocationTargetException wrapped) {
                                        throw wrapped.getCause();
                                    }
                                });
                    } catch (InvocationTargetException wrapped) {
                        throw wrapped.getCause();
                    }
                });
    }
}
