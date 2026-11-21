package org.pangrova.testing.testkit.postgres;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/** Injects a failure after a real prepared mutation, before the transaction can commit. */
public final class JdbcMutationFault {
    private JdbcMutationFault() {}

    /**
     * Delegates real JDBC work, then fails successful executeUpdate/executeBatch/executeLargeUpdate
     * calls matching the SQL prefix.
     * An optional rollback failure occurs before delegation; close still reaches the real connection.
     */
    public static DataSource afterMutation(DataSource source, String sqlPrefix,
            Throwable failure, Throwable rollbackFailure) {
        return afterSql(source, sqlPrefix, failure, rollbackFailure, true);
    }

    /** Fails after a matching Statement.execute, including a real transaction-scoped advisory lock. */
    public static DataSource afterStatement(DataSource source, String sqlPrefix,
            Throwable failure, Throwable rollbackFailure) {
        return afterSql(source, sqlPrefix, failure, rollbackFailure, false);
    }

    private static DataSource afterSql(DataSource source, String sqlPrefix,
            Throwable failure, Throwable rollbackFailure, boolean mutation) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(sqlPrefix, "sqlPrefix");
        if (!(failure instanceof SQLException || failure instanceof RuntimeException || failure instanceof Error)) {
            throw new IllegalArgumentException("failure must be a JDBC exception, runtime exception or Error");
        }
        if (rollbackFailure != null && !(rollbackFailure instanceof SQLException
                || rollbackFailure instanceof RuntimeException || rollbackFailure instanceof Error)) {
            throw new IllegalArgumentException("rollbackFailure must be a JDBC exception, runtime exception or Error");
        }
        return (DataSource) Proxy.newProxyInstance(JdbcMutationFault.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    Object result = invoke(source, method, args);
                    return result instanceof Connection connection
                            ? connection(connection, sqlPrefix, failure, rollbackFailure, mutation) : result;
                });
    }

    private static Connection connection(Connection connection, String sqlPrefix,
            Throwable failure, Throwable rollbackFailure, boolean mutation) {
        return (Connection) Proxy.newProxyInstance(JdbcMutationFault.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("rollback") && rollbackFailure != null) throw rollbackFailure;
                    Object result = invoke(connection, method, args);
                    if (mutation && method.getName().equals("prepareStatement")
                            && ((String) args[0]).startsWith(sqlPrefix)) {
                        return statement((PreparedStatement) result, failure);
                    }
                    if (!mutation && method.getName().equals("createStatement")) {
                        Statement statement = (Statement) result;
                        return Proxy.newProxyInstance(JdbcMutationFault.class.getClassLoader(),
                                new Class<?>[]{Statement.class}, (statementProxy, action, arguments) -> {
                                    Object value = invoke(statement, action, arguments);
                                    if (action.getName().equals("execute")
                                            && ((String) arguments[0]).startsWith(sqlPrefix)) throw failure;
                                    return value;
                                });
                    }
                    return result;
                });
    }

    private static PreparedStatement statement(PreparedStatement statement, Throwable failure) {
        return (PreparedStatement) Proxy.newProxyInstance(JdbcMutationFault.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    Object result = invoke(statement, method, args);
                    if (method.getName().equals("executeUpdate") || method.getName().equals("executeBatch")
                            || method.getName().equals("executeLargeUpdate")) {
                        throw failure;
                    }
                    return result;
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
}
