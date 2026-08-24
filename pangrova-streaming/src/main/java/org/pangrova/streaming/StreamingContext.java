package org.pangrova.streaming;

import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;

/**
 * Entry point wiring for the streaming module.
 * Connects MaintenanceScheduler and WatermarkTracker components.
 */
public class StreamingContext implements AutoCloseable {

    private final MaintenanceScheduler scheduler;
    private final WatermarkTracker watermarkTracker;

    public StreamingContext(MaintenanceScheduler scheduler, WatermarkTracker watermarkTracker) {
        this.scheduler = scheduler;
        this.watermarkTracker = watermarkTracker;
    }

    public MaintenanceScheduler scheduler() {
        return scheduler;
    }

    public WatermarkTracker watermarkTracker() {
        return watermarkTracker;
    }

    @Override
    public void close() throws Exception {
        scheduler.close();
    }
}
