package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
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
    private static final int STOP_TIMEOUT_MILLIS = 30_000;
    private static final String RUNNING = "SELECT * FROM ingestion_stream_state WHERE lifecycle_state = 'RUNNING'";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final ExecutorService listener = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, "cdc-stream-definition-feed"));
    private final AtomicReference<Subscription> active = new AtomicReference<>();
    private final Semaphore leadershipChanged = new Semaphore(0);

    /**
     * Uses a dedicated physical connection so LISTEN state and blocking notification
     * reads never consume a metadata pool slot or reach a different borrower.
     */
    public JdbcRunningStreamFeed(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
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
        try (var feed = PostgresChangeSubscription.open(
                jdbcUrl, user, password, "ingestion_stream_state", "pangrova_running_stream_")) {
            var connection = feed.connection();
            // LISTEN commits in autocommit mode. Any newer writes are queued even during the snapshot read.
            var definitions = load(connection);
            subscription.snapshot = Map.copyOf(definitions);
            while (active.get() == subscription && !Thread.currentThread().isInterrupted()) {
                var names = feed.awaitChanges(WAIT_MILLIS);
                if (names.contains("")) names.remove("");
                if (!names.isEmpty()) {
                    refresh(connection, definitions, names);
                    subscription.snapshot = Map.copyOf(definitions);
                }
            }
        } finally {
            subscription.snapshot = Map.of();
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
