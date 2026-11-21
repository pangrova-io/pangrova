package org.pangrova.operations.diagnostics;

import org.pangrova.operations.exception.DiagnosticException;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;

/**
 * Controls JFR recording sessions using the JDK Flight Recorder API.
 * Manages lifecycle including auto-scavenging of recordings that stop
 * naturally (via duration limit) and explicit close-all cleanup.
 */
final class JfrController implements AutoCloseable {

    private final Map<Long, RecordingEntry> activeRecordings = new HashMap<>();
    private boolean closed = false;
    private final ScheduledExecutorService scavenger;
    private final ScheduledFuture<?> scavengerFuture;

    JfrController() {
        scavenger = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jfr-scavenger");
            t.setDaemon(true);
            return t;
        });
        scavengerFuture = scavenger.scheduleAtFixedRate(
                this::scavengeStoppedRecordings, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Starts a new JFR recording with the given duration and settings.
     *
     * @return the recording ID assigned by JFR
     */
    synchronized long start(Duration duration, String settings, Path outputPath) throws Exception {
        if (closed) {
            throw new DiagnosticException("JfrController is closed");
        }
        Configuration config = Configuration.getConfiguration(settings);
        Recording recording = new Recording(config);
        try {
            recording.setMaxAge(duration);
            recording.setDuration(duration);
            recording.setDestination(outputPath);
            recording.start();
            long id = recording.getId();
            activeRecordings.put(id, new RecordingEntry(recording, outputPath));
            return id;
        } catch (Exception | Error failure) {
            try {
                recording.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Stops an active JFR recording.
     *
     * @return the output path where the recording was written
     */
    synchronized Path stop(long recordingId) throws DiagnosticException {
        RecordingEntry entry = activeRecordings.remove(recordingId);
        if (entry == null) {
            throw new DiagnosticException("No active recording with id=" + recordingId);
        }
        try (var recording = entry.recording()) {
            if (recording.getState() == RecordingState.RUNNING) {
                recording.stop();
            }
        }
        return entry.outputPath();
    }

    /**
     * Returns the number of currently active (tracked) recordings.
     */
    synchronized int activeRecordingCount() {
        return activeRecordings.size();
    }

    /**
     * Stops all active recordings and shuts down the scavenger.
     * Idempotent: calling close() multiple times is safe.
     */
    @Override
    public synchronized void close() {
        closed = true;
        scavengerFuture.cancel(false);
        scavenger.shutdownNow();
        for (var entry : activeRecordings.values()) {
            try (var rec = entry.recording()) {
                if (rec.getState() == RecordingState.RUNNING) {
                    rec.stop();
                }
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        activeRecordings.clear();
    }

    private synchronized void scavengeStoppedRecordings() {
        activeRecordings.entrySet().removeIf(e -> {
            RecordingState state = e.getValue().recording().getState();
            if (state == RecordingState.STOPPED || state == RecordingState.CLOSED) {
                try {
                    e.getValue().recording().close();
                } catch (Exception ignored) {
                    // already stopped
                }
                return true;
            }
            return false;
        });
    }

    private record RecordingEntry(Recording recording, Path outputPath) {}
}
