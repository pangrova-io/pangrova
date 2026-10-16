package org.pangrova.streaming.query;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically polls registered continuous queries and invokes the executor
 * to refresh materialized views or incremental aggregations.
 */
public class ContinuousQueryConsumer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousQueryConsumer.class);

    private final ConcurrentMap<String, Duration> queries;
    private final ContinuousQueryExecutor executor;
    private final Duration scanInterval;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledQuery> tasks = new ConcurrentHashMap<>();

    /** Creates a consumer that schedules the given continuous queries at their declared intervals. */
    public ContinuousQueryConsumer(
            ConcurrentMap<String, Duration> queries,
            ContinuousQueryExecutor executor,
            Duration scanInterval) {
        // CREATE/DROP can occur after startup, so the registration map must stay live.
        this.queries = Objects.requireNonNull(queries, "queries must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.scanInterval = Objects.requireNonNull(scanInterval, "scanInterval must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cq-consumer");
            t.setDaemon(true);
            return t;
        });
    }

    /** Starts the scan loop that discovers and schedules continuous queries. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::scan, 0,
                scanInterval.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("event=continuous_query_consumer_started interval={}", scanInterval);
    }

    void scan() {
        tasks.forEach((name, scheduled) -> {
            if (!scheduled.interval().equals(queries.get(name))) {
                if (tasks.remove(name, scheduled)) {
                    scheduled.future().cancel(false);
                }
            }
        });
        for (var entry : queries.entrySet()) {
            String name = entry.getKey();
            Duration interval = entry.getValue();
            tasks.computeIfAbsent(name, n ->
                    new ScheduledQuery(interval, scheduler.scheduleAtFixedRate(
                            () -> safeExecute(n),
                            0, interval.toMillis(), TimeUnit.MILLISECONDS)));
        }
    }

    private void safeExecute(String name) {
        if (!queries.containsKey(name)) {
            return;
        }
        try {
            executor.executeRefresh(name);
        } catch (Exception e) {
            LOG.warn("event=continuous_query_refresh_failed query={}", name, e);
        }
    }

    int scheduledQueryCount() {
        return tasks.size();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        tasks.clear();
        LOG.info("event=continuous_query_consumer_closed");
    }

    private record ScheduledQuery(Duration interval, ScheduledFuture<?> future) {}
}
