package org.pangrova.persistence.security.postgresql.transaction;

import java.sql.Connection;
import java.sql.SQLException;

/** PostgreSQL transaction lock shared by publication and every authority mutation. */
public final class AuthorizationTransactionGate {
    private static final String SHARED_LOCK = "SELECT pg_advisory_xact_lock_shared(207578817, 1)";
    private static final String EXCLUSIVE_LOCK = "SELECT pg_advisory_xact_lock(207578817, 1)";

    private AuthorizationTransactionGate() {}

    /** Acquires the gate before any job, descriptor or publication row lock. */
    public static void lockPublication(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Publication authority requires an explicit transaction");
        }
        execute(connection, SHARED_LOCK);
    }

    /** Acquires the mutation gate before schema or authority row locks. Ordinary query reads do not use it. */
    public static void lockMutation(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Authority mutation requires an explicit transaction");
        }
        execute(connection, EXCLUSIVE_LOCK);
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
