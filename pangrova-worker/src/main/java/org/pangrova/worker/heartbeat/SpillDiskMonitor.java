package org.pangrova.worker.heartbeat;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reports available disk space on the configured spill directory.
 *
 * <p>Used by the heartbeat payload supplier to populate the
 * {@code spill_disk_available_bytes} metric, giving the Coordinator
 * visibility into whether the worker can still spill to disk.</p>
 *
 * <p>Thread-safe: filesystem space queries are safe to call
 * from any thread.</p>
 */
@ThreadSafe
public final class SpillDiskMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(SpillDiskMonitor.class);

    private final Path spillDir;

    /**
     * Creates a monitor for the given spill directory path.
     *
     * @param spillDirectory path to the spill directory (may be null if not configured)
     */
    public SpillDiskMonitor(String spillDirectory) {
        this.spillDir = spillDirectory != null && !spillDirectory.isBlank()
                ? Path.of(spillDirectory) : null;
    }

    /**
     * Returns the number of bytes available on the spill directory's filesystem.
     *
     * <p>Returns 0 if the spill directory is not configured, does not exist,
     * or if the check fails.</p>
     *
     * @return available bytes on the spill disk, or 0 if unavailable
     */
    public long getAvailableBytes() {
        if (spillDir == null || !Files.isDirectory(spillDir)) {
            return 0;
        }
        try {
            long usable = Files.getFileStore(spillDir).getUsableSpace();
            return usable >= 0 ? usable : 0;
        } catch (IOException | SecurityException e) {
            LOG.debug("event=spill_disk_check_failed error={}", e.getMessage());
            return 0;
        }
    }

    /**
     * Returns whether a spill directory is configured and exists on disk.
     */
    public boolean isConfigured() {
        return spillDir != null && Files.isDirectory(spillDir);
    }
}
