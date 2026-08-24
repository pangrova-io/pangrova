package org.pangrova.operations.diagnostics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Diagnostic endpoint providing threaddump, heapdump, and JFR operations
 * for SRE troubleshooting.
 *
 * <p>Threaddump returns a textual thread dump of all JVM threads.
 * Heapdump writes a heap dump to the configured output directory.
 * JFR start/stop controls Java Flight Recorder sessions for profiling.
 */
public final class DiagnosticEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticEndpoint.class);

    private static final int THREAD_DUMP_INITIAL_CAPACITY = 4096;

    /** Limits concurrent heavy diagnostic operations (heapdump, JFR) to one at a time. */
    private final Semaphore heavyOperationPermit = new Semaphore(1);

    private final DiagnosticConfig config;
    private final JfrController jfrController;
    private final HeapDumpWriter heapDumpWriter;

    public DiagnosticEndpoint(DiagnosticConfig config) {
        this(config, new JfrController(), new HeapDumpWriter());
    }

    DiagnosticEndpoint(DiagnosticConfig config, JfrController jfrController, HeapDumpWriter heapDumpWriter) {
        this.config = Objects.requireNonNull(config, "config");
        this.jfrController = Objects.requireNonNull(jfrController, "jfrController");
        this.heapDumpWriter = Objects.requireNonNull(heapDumpWriter, "heapDumpWriter");
    }

    /**
     * GET /v1/diagnostics/threaddump
     *
     * <p>Returns a textual thread dump of all JVM threads including lock info.
     */
    public ThreadDumpResult threadDump() {
        LOG.info("Thread dump requested");
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(
                threadMXBean.isObjectMonitorUsageSupported(),
                threadMXBean.isSynchronizerUsageSupported());

        StringBuilder sb = new StringBuilder(THREAD_DUMP_INITIAL_CAPACITY);
        sb.append("Thread dump at ").append(Instant.now()).append('\n');
        sb.append("Total threads: ").append(threadInfos.length).append("\n\n");

        for (ThreadInfo info : threadInfos) {
            sb.append(info.toString());
        }

        return new ThreadDumpResult(threadInfos.length, sb.toString(), Instant.now());
    }

    /**
     * POST /v1/diagnostics/heapdump
     *
     * <p>Writes a heap dump to the configured output directory. Only one heap dump
     * can be in progress at a time.
     */
    public HeapDumpResult heapDump() throws DiagnosticException {
        if (!heavyOperationPermit.tryAcquire()) {
            throw new DiagnosticException("Another diagnostic operation is in progress, please retry later");
        }
        try {
            return doHeapDump();
        } finally {
            heavyOperationPermit.release();
        }
    }

    private HeapDumpResult doHeapDump() throws DiagnosticException {
        LOG.info("Heap dump requested, outputDir={}", config.heapDumpOutputDir());

        Path outputDir = config.heapDumpOutputDir();
        ensureDirectoryExists(outputDir);

        String filename = "heapdump-" + Instant.now().toEpochMilli() + ".hprof";
        Path outputPath = outputDir.resolve(filename);

        try {
            heapDumpWriter.write(outputPath, config.heapDumpLiveObjectsOnly());
            long fileSize = Files.size(outputPath);
            LOG.info("Heap dump written, path={}, sizeBytes={}", outputPath, fileSize);
            return new HeapDumpResult(outputPath.toString(), fileSize, Instant.now());
        } catch (IOException e) {
            throw new DiagnosticException("Failed to write heap dump to " + outputPath, e);
        }
    }

    /**
     * POST /v1/diagnostics/jfr/start
     *
     * <p>Starts a JFR recording session with the configured duration and settings.
     */
    public JfrResult jfrStart(JfrStartRequest request) throws DiagnosticException {
        if (!heavyOperationPermit.tryAcquire()) {
            throw new DiagnosticException("Another diagnostic operation is in progress, please retry later");
        }
        try {
            return doJfrStart(request);
        } finally {
            heavyOperationPermit.release();
        }
    }

    private JfrResult doJfrStart(JfrStartRequest request) throws DiagnosticException {
        Duration duration = request.duration() != null ? request.duration() : config.jfrDefaultDuration();
        String settings = request.settings() != null ? request.settings() : config.jfrDefaultSettings();

        LOG.info("JFR start requested, duration={}, settings={}", duration, settings);

        Path outputDir = config.jfrOutputDir();
        ensureDirectoryExists(outputDir);

        String filename = "recording-" + Instant.now().toEpochMilli() + ".jfr";
        Path outputPath = outputDir.resolve(filename);

        try {
            long recordingId = jfrController.start(duration, settings, outputPath);
            LOG.info("JFR recording started, recordingId={}, output={}", recordingId, outputPath);
            return new JfrResult(recordingId, JfrState.RUNNING, outputPath.toString(),
                    duration, Instant.now());
        } catch (Exception e) {
            throw new DiagnosticException("Failed to start JFR recording", e);
        }
    }

    /**
     * POST /v1/diagnostics/jfr/stop
     *
     * <p>Stops an active JFR recording session.
     */
    public JfrResult jfrStop(JfrStopRequest request) throws DiagnosticException {
        LOG.info("JFR stop requested, recordingId={}", request.recordingId());

        try {
            Path outputPath = jfrController.stop(request.recordingId());
            long fileSize = Files.exists(outputPath) ? Files.size(outputPath) : 0;
            LOG.info("JFR recording stopped, recordingId={}, output={}, sizeBytes={}",
                    request.recordingId(), outputPath, fileSize);
            return new JfrResult(request.recordingId(), JfrState.STOPPED, outputPath.toString(),
                    Duration.ZERO, Instant.now());
        } catch (Exception e) {
            throw new DiagnosticException(
                    "Failed to stop JFR recording " + request.recordingId(), e);
        }
    }

    private void ensureDirectoryExists(Path dir) throws DiagnosticException {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new DiagnosticException("Cannot create output directory: " + dir, e);
        }
    }
}
