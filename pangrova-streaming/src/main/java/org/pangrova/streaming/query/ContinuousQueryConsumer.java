package org.pangrova.streaming.query;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<String, Duration> queries;
    private final ContinuousQueryExecutor executor;
    private final Duration scanInterval;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

    /** Creates a consumer that schedules the given continuous queries at their declared intervals. */
    public ContinuousQueryConsumer(
            Map<String, Duration> queries,
            ContinuousQueryExecutor executor,
            Duration scanInterval) {
        this.queries = queries;
        this.executor = executor;
        this.scanInterval = scanInterval;
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

    private void scan() {
        for (var entry : queries.entrySet()) {
            String name = entry.getKey();
            Duration interval = entry.getValue();
            tasks.computeIfAbsent(name, n ->
                    scheduler.scheduleAtFixedRate(
                            () -> safeExecute(n),
                            0, interval.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    private void safeExecute(String name) {
        try {
            executor.executeRefresh(name);
        } catch (Exception e) {
            LOG.warn("event=continuous_query_refresh_failed query={}", name, e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        tasks.clear();
        LOG.info("event=continuous_query_consumer_closed");
    }
}
