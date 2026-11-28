package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Projects committed running stream definitions for the active maintenance Coordinator. */
@ThreadSafe
public final class JdbcRunningStreamFeed implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcRunningStreamFeed.class);
    private static final int WAIT_MILLIS = 1_000;
    private static final int NETWORK_TIMEOUT_MILLIS = 5_000;
    private static final int STOP_TIMEOUT_MILLIS = 30_000;
    private static final String RUNNING = "SELECT * FROM ingestion_stream_state WHERE lifecycle_state = 'RUNNING'";

    private final PGSimpleDataSource dedicatedSource = new PGSimpleDataSource();
    private final ExecutorService listener = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, "cdc-stream-definition-feed"));
    private final AtomicReference<Subscription> active = new AtomicReference<>();
    private final Semaphore leadershipChanged = new Semaphore(0);

    /**
     * Uses a dedicated physical connection so LISTEN state and blocking notification
     * reads never consume a metadata pool slot or reach a different borrower.
     */
    public JdbcRunningStreamFeed(String jdbcUrl, String user, String password) {
        dedicatedSource.setURL(jdbcUrl);
        dedicatedSource.setUser(user);
        dedicatedSource.setPassword(password);
        dedicatedSource.setTcpKeepAlive(true);
    }

    public void start() {
        listener.execute(this::followLeadership);
    }

    public void setActive(boolean leader) {
        if (leader ? active.compareAndSet(null, new Subscription()) : active.getAndSet(null) != null) {
            leadershipChanged.release();
        }
    }

    public Map<String, StreamState> snapshot() {
        var current = active.get();
        return current == null ? Map.of() : current.snapshot;
    }

    @Override
    public void close() {
        setActive(false);
        listener.shutdownNow();
        ExecutorDrain.shutdownAndAwait(listener, STOP_TIMEOUT_MILLIS);
    }

    private void followLeadership() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                var current = active.get();
                if (current == null) {
                    leadershipChanged.acquire();
                    continue;
                }
                leadershipChanged.drainPermits();
                try {
                    follow(current);
                } catch (SQLException failure) {
                    if (active.get() == current && !Thread.currentThread().isInterrupted()) {
                        LOG.warn("event=cdc_stream_feed_disconnected", failure);
                        leadershipChanged.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } catch (InterruptedException shutdown) {
            Thread.currentThread().interrupt();
        }
    }

    private void follow(Subscription subscription) throws SQLException {
        try (Connection connection = dedicatedSource.getConnection()) {
            connection.setNetworkTimeout(Runnable::run, NETWORK_TIMEOUT_MILLIS);
            var notifications = connection.unwrap(PGConnection.class);
            String channel = subscribe(connection);
            // LISTEN commits in autocommit mode. Any newer writes are queued even during the snapshot read.
            var definitions = load(connection);
            subscription.snapshot = Map.copyOf(definitions);
            while (active.get() == subscription && !Thread.currentThread().isInterrupted()) {
                var names = changedNames(channel, notifications.getNotifications(WAIT_MILLIS));
                if (!names.isEmpty()) {
                    refresh(connection, definitions, names);
                    subscription.snapshot = Map.copyOf(definitions);
                }
            }
        } finally {
            subscription.snapshot = Map.of();
        }
    }

    private static String subscribe(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            String channel;
            try (var rows = statement.executeQuery("SELECT 'ingestion_stream_state'::regclass::oid")) {
                if (!rows.next()) throw new SQLException("Missing CDC stream relation identity");
                channel = "pangrova_running_stream_" + rows.getLong(1);
            }
            statement.execute("LISTEN " + channel);
            return channel;
        }
    }

    private static Map<String, StreamState> load(Connection connection) throws SQLException {
        var definitions = new HashMap<String, StreamState>();
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(RUNNING)) {
            for (var state : JdbcStreamStateStore.mapRows(rows)) {
                definitions.put(state.streamName(), state);
            }
        }
        return definitions;
    }

    private static Set<String> changedNames(String channel, PGNotification[] notifications) {
        if (notifications == null || notifications.length == 0) return Set.of();
        var names = new HashSet<String>();
        for (var event : notifications) {
            if (channel.equals(event.getName()) && !event.getParameter().isEmpty()) {
                names.add(event.getParameter());
            }
        }
        return names;
    }

    private static void refresh(Connection connection, Map<String, StreamState> definitions,
            Set<String> names) throws SQLException {
        var array = connection.createArrayOf("varchar", names.toArray(String[]::new));
        try (var statement = connection.prepareStatement(RUNNING + " AND stream_name = ANY (?)")) {
            statement.setArray(1, array);
            try (var rows = statement.executeQuery()) {
                definitions.keySet().removeAll(names);
                for (var state : JdbcStreamStateStore.mapRows(rows)) {
                    definitions.put(state.streamName(), state);
                }
            }
        } finally {
            array.free();
        }
    }

    private static final class Subscription {
        private volatile Map<String, StreamState> snapshot = Map.of();
    }
}
