package org.pangrova.query.store.jdbc.lineage;

import org.pangrova.spi.query.lineage.QueryLineageFact;
import org.pangrova.persistence.api.query.lineage.QueryLineageOutbox;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** PostgreSQL outbox shared with the authoritative journal; callers retain ownership of the DataSource. */
public final class JdbcQueryLineageOutbox implements QueryLineageOutbox {
    private static final long MAX_LEASE_MINUTES = 5;
    private final DataSource dataSource;

    /** Uses the externally owned metadata DataSource after production migrations have completed. */
    public JdbcQueryLineageOutbox(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void append(QueryLineageFact fact) {
        Objects.requireNonNull(fact, "fact");
        transaction(connection -> {
            QueryLineageFactSql.append(connection, fact);
            return null;
        });
    }

    @Override
    public Optional<Delivery> claim(String namespace, Duration lease) {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isBlank() || namespace.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Lineage claim requires its exact deployment namespace");
        }
        Objects.requireNonNull(lease, "lease");
        long leaseMs = lease.toMillis();
        if (leaseMs <= 0 || leaseMs > Duration.ofMinutes(MAX_LEASE_MINUTES).toMillis()) {
            throw new IllegalArgumentException("Lineage delivery lease must be within (0, 5 minutes]");
        }
        return transaction(connection -> claim(connection, namespace, leaseMs));
    }

    private Optional<Delivery> claim(Connection connection, String namespace, long leaseMs) throws SQLException {
        QueryLineageFact fact;
        try (PreparedStatement select = connection.prepareStatement("""
                SELECT pending.event_id, pending.query_id, pending.category, pending.payload::text
                FROM query_lineage_outbox pending
                WHERE pending.namespace = ? AND pending.delivered_at IS NULL
                  AND (pending.lease_until IS NULL OR pending.lease_until <= clock_timestamp())
                  AND (pending.category = 'START' OR EXISTS (
                      SELECT 1 FROM query_lineage_outbox started
                      WHERE started.namespace = pending.namespace AND started.query_id = pending.query_id
                        AND started.category = 'START' AND started.delivered_at IS NOT NULL))
                ORDER BY pending.created_at_ms, pending.event_id
                FOR UPDATE OF pending SKIP LOCKED LIMIT 1
                """)) {
            select.setString(1, namespace);
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                fact = QueryLineageFactSql.decode(rows.getString("payload"));
                String category = fact.phase() == QueryLineageFact.Phase.START ? "START" : "TERMINAL";
                if (!namespace.equals(fact.namespace()) || !fact.queryId().equals(rows.getString("query_id"))
                        || !category.equals(rows.getString("category"))
                        || !fact.eventId().equals(rows.getObject("event_id", java.util.UUID.class))) {
                    throw new SQLException("Durable lineage row identity differs from its frozen payload");
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE query_lineage_outbox SET delivery_token = delivery_token + 1,
                    lease_until = clock_timestamp() + (? * interval '1 millisecond')
                WHERE event_id = ? AND delivered_at IS NULL RETURNING delivery_token
                """)) {
            update.setLong(1, leaseMs);
            update.setObject(2, fact.eventId());
            try (ResultSet rows = update.executeQuery()) {
                if (!rows.next()) throw new SQLException("Locked lineage delivery disappeared");
                return Optional.of(new Delivery(fact, rows.getLong(1)));
            }
        }
    }

    @Override
    public void acknowledge(Delivery delivery) {
        Objects.requireNonNull(delivery, "delivery");
        transaction(connection -> {
            OwnedRow row = lock(connection, delivery);
            if (row.delivered()) return null;
            requireLease(connection, row);
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE query_lineage_outbox SET delivered_at = clock_timestamp(), lease_until = NULL
                    WHERE event_id = ? AND delivery_token = ? AND delivered_at IS NULL
                    """)) {
                update.setObject(1, delivery.fact().eventId());
                update.setLong(2, delivery.token());
                if (update.executeUpdate() != 1) throw new SQLException("Lineage acknowledgement lost ownership");
            }
            // A row lock wait and every owned SQL mutation precede this final independent clock read.
            requireLease(connection, row);
            return null;
        });
    }

    @Override
    public void release(Delivery delivery) {
        Objects.requireNonNull(delivery, "delivery");
        transaction(connection -> {
            OwnedRow row = lock(connection, delivery);
            if (!row.delivered()) {
                try (PreparedStatement update = connection.prepareStatement("""
                        UPDATE query_lineage_outbox SET lease_until = NULL
                        WHERE event_id = ? AND delivery_token = ? AND delivered_at IS NULL
                        """)) {
                    update.setObject(1, delivery.fact().eventId());
                    update.setLong(2, delivery.token());
                    if (update.executeUpdate() != 1) throw new SQLException("Lineage release lost ownership");
                }
            }
            return null;
        });
    }

    private static OwnedRow lock(Connection connection, Delivery delivery) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement("""
                SELECT payload::text, delivery_token, lease_until, delivered_at
                FROM query_lineage_outbox WHERE event_id = ? FOR UPDATE
                """)) {
            select.setObject(1, delivery.fact().eventId());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next() || rows.getLong("delivery_token") != delivery.token()
                        || !delivery.fact().equals(QueryLineageFactSql.decode(rows.getString("payload")))) {
                    throw new SQLException("Stale or conflicting lineage delivery token", "40001");
                }
                var lease = rows.getTimestamp("lease_until");
                return new OwnedRow(lease == null ? null : lease.toInstant(),
                        rows.getTimestamp("delivered_at") != null);
            }
        }
    }

    private static void requireLease(Connection connection, OwnedRow row) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement("SELECT clock_timestamp()");
                ResultSet clock = select.executeQuery()) {
            if (!clock.next() || row.leaseUntil() == null
                    || !row.leaseUntil().isAfter(clock.getTimestamp(1).toInstant())) {
                throw new SQLException("Lineage delivery lease expired before acknowledgement commit", "40001");
            }
        }
    }

    @Override
    public long pendingCount() {
        return transaction(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT count(*) FROM query_lineage_outbox WHERE delivered_at IS NULL");
                    ResultSet rows = select.executeQuery()) {
                if (!rows.next()) throw new SQLException("Missing lineage pending count");
                return rows.getLong(1);
            }
        });
    }

    private <T> T transaction(SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.perform(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    if (rollback != failure) failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Durable query lineage operation was not established", failure);
        }
    }

    private record OwnedRow(Instant leaseUntil, boolean delivered) { }

    @FunctionalInterface
    private interface SqlWork<T> { T perform(Connection connection) throws SQLException; }
}
