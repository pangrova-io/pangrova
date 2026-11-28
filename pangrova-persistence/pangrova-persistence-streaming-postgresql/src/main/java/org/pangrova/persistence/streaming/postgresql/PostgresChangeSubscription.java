package org.pangrova.persistence.streaming.postgresql;

import org.postgresql.PGConnection;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/** Holds a dedicated LISTEN connection for committed changes to one PostgreSQL relation. */
public final class PostgresChangeSubscription implements AutoCloseable {
    private static final int NETWORK_TIMEOUT_MILLIS = 5_000;

    private final Connection connection;
    private final PGConnection notifications;
    private final String channel;

    private PostgresChangeSubscription(Connection connection, String channel) throws SQLException {
        this.connection = connection;
        this.notifications = connection.unwrap(PGConnection.class);
        this.channel = channel;
    }

    /** Subscribes to committed changes on the named relation. */
    public static PostgresChangeSubscription open(String jdbcUrl, String user, String password,
            String relation, String channelPrefix) throws SQLException {
        if (!channelPrefix.matches("[a-z_]+")) throw new IllegalArgumentException("Invalid notification prefix");
        var source = new PGSimpleDataSource();
        source.setURL(jdbcUrl);
        source.setUser(user);
        source.setPassword(password);
        source.setTcpKeepAlive(true);
        var connection = source.getConnection();
        try {
            connection.setNetworkTimeout(Runnable::run, NETWORK_TIMEOUT_MILLIS);
            String channel;
            try (var identity = connection.prepareStatement("SELECT CAST(? AS regclass)::oid")) {
                identity.setString(1, relation);
                try (var rows = identity.executeQuery()) {
                    if (!rows.next()) throw new SQLException("Missing notification relation identity");
                    channel = channelPrefix + rows.getLong(1);
                }
            }
            try (var statement = connection.createStatement()) {
                statement.execute("LISTEN " + channel);
            }
            return new PostgresChangeSubscription(connection, channel);
        } catch (SQLException | RuntimeException failure) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    public Connection connection() {
        return connection;
    }

    /** Waits for notifications and returns their distinct payloads. */
    public Set<String> awaitChanges(int waitMillis) throws SQLException {
        var received = notifications.getNotifications(waitMillis);
        if (received == null || received.length == 0) return Set.of();
        var changes = new HashSet<String>();
        for (var event : received) {
            if (channel.equals(event.getName())) changes.add(event.getParameter());
        }
        return changes;
    }

    /** Interrupts an in-flight socket read during owner shutdown. */
    public void abort() throws SQLException {
        connection.abort(Runnable::run);
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
