package org.pangrova.persistence.jdbc.support;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcRollbackTest {
    @Test
    void rollbackRetainsTheOriginalFailureAndLeavesCloseToTheOwner() {
        RuntimeException primary = new IllegalStateException("write failed");
        Throwable[] cleanupFailures = {new SQLException("connection lost"), new IllegalStateException("pool closed"),
                new AssertionError("driver failure"), primary};
        for (Throwable cleanup : cleanupFailures) {
            var calls = new java.util.ArrayList<String>();
            var connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                        calls.add(method.getName());
                        throw cleanup;
                    });
            JdbcSupport.rollbackAfterFailure(connection, primary);
            assertThat(calls).containsExactly("rollback");
        }
        assertThat(List.of(primary.getSuppressed())).containsExactly(
                cleanupFailures[0], cleanupFailures[1], cleanupFailures[2]);
    }
}
