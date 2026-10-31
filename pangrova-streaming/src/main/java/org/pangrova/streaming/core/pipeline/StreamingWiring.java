package org.pangrova.streaming.core.pipeline;

import org.pangrova.connector.spi.data.write.TableWriter;
import org.pangrova.streaming.core.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.core.watermark.WatermarkTracker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entry point wiring for the streaming module.
 * Registers TableWriter implementations and connects components.
 */
public class StreamingWiring implements AutoCloseable {

    private final Map<String, TableWriter> writers = new ConcurrentHashMap<>();
    private final MaintenanceScheduler scheduler;
    private final WatermarkTracker watermarkTracker;

    public StreamingWiring(MaintenanceScheduler scheduler, WatermarkTracker watermarkTracker) {
        this.scheduler = scheduler;
        this.watermarkTracker = watermarkTracker;
    }

    public void registerWriter(String format, TableWriter writer) {
        writers.put(format, writer);
    }

    public TableWriter getWriter(String format) {
        return writers.get(format);
    }

    public MaintenanceScheduler scheduler() {
        return scheduler;
    }

    public WatermarkTracker watermarkTracker() {
        return watermarkTracker;
    }

    /**
     * Closes all registered writers and the scheduler, collecting all exceptions.
     * Ensures every resource gets a close attempt even if earlier ones fail.
     */
    @Override
    public void close() throws Exception {
        Exception firstException = null;
        for (TableWriter writer : writers.values()) {
            firstException = closeAccumulating(writer, firstException);
        }
        firstException = closeAccumulating(scheduler, firstException);
        // WatermarkTracker does not currently hold resources, but closing
        // defensively ensures future resource additions are covered.
        if (firstException != null) {
            throw firstException;
        }
    }

    private static Exception closeAccumulating(AutoCloseable resource, Exception accumulated) {
        try {
            resource.close();
        } catch (Exception e) {
            if (accumulated == null) {
                return e;
            }
            accumulated.addSuppressed(e);
        }
        return accumulated;
    }
}
