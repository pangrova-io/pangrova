package org.pangrova.streaming.worker;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.protocol.task.WorkerTerminalTaskReports;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.CdcWriteOperation;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.streaming.exception.CdcIngestCancelledException;
import org.pangrova.streaming.cdc.CdcRangeIngestResult;
import org.pangrova.streaming.cdc.CdcRangeIngestSpec;
import org.pangrova.streaming.cdc.CdcRangeIngestor;
import org.pangrova.streaming.cdc.CdcTaskRange;
import org.pangrova.worker.execution.WorkerExecutedTaskBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker-owned execution of one bounded CDC ingest task.
 *
 * <p>This backend reads only the admitted ranges, stages decoded batches through
 * the injected writer, and returns either the canonical successful JVM receipt
 * or a fail-closed unsuccessful receipt. It never commits a catalog snapshot or
 * acknowledges a source offset.</p>
 */
@Experimental
public final class CdcWorkerTaskBackend implements WorkerExecutedTaskBackend {

    private static final Logger LOG = LoggerFactory.getLogger(CdcWorkerTaskBackend.class);
    private static final int MAX_ERROR_MESSAGE_LENGTH = 4096;
    private static final long SHUTDOWN_TIMEOUT_MS = 30_000L;

    private final CdcWorkerTaskResourcesProvider resources;
    private final CdcWorkerTaskOptions options;
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "worker-cdc-task");
        thread.setDaemon(true);
        return thread;
    });
    private final ConcurrentHashMap<Long, Session> sessions = new ConcurrentHashMap<>();
    private final AtomicLong nextHandle = new AtomicLong();
    private boolean closed;

    public CdcWorkerTaskBackend(
            CdcWorkerTaskResourcesProvider resources, CdcWorkerTaskOptions options) {
        this.resources = Objects.requireNonNull(resources, "resources");
        this.options = Objects.requireNonNull(options, "options");
    }

    @Override
    public boolean supports(TaskKind taskKind) {
        return taskKind == TaskKind.TASK_KIND_CDC_INGEST;
    }

    @Override
    public synchronized long create(TaskRequest request) {
        if (closed) {
            throw new IllegalStateException("Worker CDC backend is closed");
        }
        Objects.requireNonNull(request, "request");
        if (request.getTaskKind() != TaskKind.TASK_KIND_CDC_INGEST
                || !request.hasCdcIngest() || !request.hasAttempt()) {
            throw new IllegalArgumentException(
                    "Worker CDC backend requires a complete CDC ingest task");
        }
        CdcIngestBinding binding = request.getCdcIngest();
        if (!binding.getTenantId().equals(request.getSecurity().getTenantId())
                || binding.getStreamId().isBlank()
                || binding.getSourceOwnershipEpoch().isBlank()
                || binding.getRangesCount() == 0
                || binding.getAllowedOperationsCount() == 0) {
            throw new IllegalArgumentException(
                    "Worker CDC binding differs from its admitted task identity");
        }
        CdcRangeIngestSpec spec = spec(binding);
        long handle = nextHandle.incrementAndGet();
        sessions.put(handle, new Session(request, spec));
        return handle;
    }

    private CdcRangeIngestSpec spec(CdcIngestBinding binding) {
        List<CdcTaskRange> ranges = new ArrayList<>(binding.getRangesCount());
        for (CdcSourceRange range : binding.getRangesList()) {
            ranges.add(new CdcTaskRange(
                    range.getTopic(), range.getPartition(),
                    range.getStartOffset(), range.getEndOffset()));
        }
        return new CdcRangeIngestSpec(binding.getStreamId(),
                binding.getSourceOwnershipEpoch(), ranges,
                options.maxRecordsPerPoll(), options.pollTimeout(), options.idleTimeout());
    }

    @Override
    public void start(long handle) {
        requireSession(handle).start();
    }

    @Override
    public boolean isFinished(long handle) {
        return requireSession(handle).isFinished();
    }

    @Override
    public byte[] getTaskStatus(long handle) {
        return requireSession(handle).requireTerminalReport();
    }

    @Override
    public void cancel(long handle) {
        requireSession(handle).cancel();
    }

    @Override
    public List<byte[]> getCommitFragments(long handle) {
        return requireSession(handle).commitFragments();
    }

    @Override
    public Optional<byte[]> release(long handle) {
        Session session = sessions.get(handle);
        if (session == null) {
            return Optional.empty();
        }
        byte[] report = session.requireTerminalReport();
        sessions.remove(handle, session);
        return Optional.of(report);
    }

    @Override
    public synchronized void close() {
        closed = true;
        sessions.values().forEach(Session::cancel);
        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(executor, SHUTDOWN_TIMEOUT_MS);
        sessions.clear();
    }

    private Session requireSession(long handle) {
        Session session = sessions.get(handle);
        if (session == null) {
            throw new IllegalStateException("Worker CDC handle is not tracked: " + handle);
        }
        return session;
    }

    private final class Session {
        private final TaskRequest request;
        private final CdcRangeIngestSpec spec;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private boolean started;
        private Thread runner;
        private volatile byte[] terminalReport;
        private volatile List<byte[]> stagedFragments = List.of();

        private Session(TaskRequest request, CdcRangeIngestSpec spec) {
            this.request = request;
            this.spec = spec;
        }

        private synchronized void start() {
            if (terminalReport != null) {
                return;
            }
            if (started) {
                throw new IllegalStateException("Worker CDC task was already started");
            }
            executor.execute(this::run);
            started = true;
        }

        private boolean isFinished() {
            return terminalReport != null;
        }

        private synchronized void cancel() {
            if (terminalReport != null) return;
            cancelled.set(true);
            if (runner == null) {
                terminalReport = cancelledReport();
            } else {
                runner.interrupt();
            }
        }

        private byte[] cancelledReport() {
            return failure(TaskState.TASK_STATE_CANCELLED, ErrorClass.ERROR_CLASS_CANCELLED,
                    "TASK_CANCELLED", "CDC task was cancelled by the control plane");
        }

        private List<byte[]> commitFragments() {
            return stagedFragments.stream().map(byte[]::clone).toList();
        }

        private byte[] requireTerminalReport() {
            byte[] report = terminalReport;
            if (report == null) {
                throw new IllegalStateException("CDC task has no terminal report yet");
            }
            return report;
        }

        private void run() {
            synchronized (this) {
                if (terminalReport != null) return;
                runner = Thread.currentThread();
            }
            byte[] report;
            List<byte[]> fragments = List.of();
            try {
                CdcRangeIngestResult result;
                try (var opened = resources.open(request)) {
                    Set<CdcWriteOperation> allowed = Set.copyOf(request.getCdcIngest().getAllowedOperationsList());
                    result = new CdcRangeIngestor(spec, opened.sourceReader(), opened.converter(), envelopes -> {
                        for (var envelope : envelopes) {
                            var operation = CdcWriteOperation.valueOf(
                                    "CDC_WRITE_OPERATION_" + envelope.operation().name());
                            if (!allowed.contains(operation)) {
                                throw new IllegalArgumentException(
                                        "CDC stream is not authorized for " + envelope.operation());
                            }
                        }
                        return opened.writer().writeBatch(envelopes);
                    }, cancelled::get).ingest(opened.writer().requiresSingleRange());
                }
                fragments = result.commitFragments();
                report = WorkerTerminalTaskReports.workerExecuted(request);
                LOG.info("event=worker_cdc_task_completed stream={} records={} fragments={}",
                        spec.streamId(), result.consumedRecordCount(), fragments.size());
            } catch (CdcIngestCancelledException cancelledTask) {
                report = cancelledReport();
            } catch (RuntimeException | Error failure) {
                report = failure(TaskState.TASK_STATE_FAILED,
                        ErrorClass.ERROR_CLASS_INTERNAL, "CDC_INGEST_FAILED",
                        failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage());
                LOG.error("event=worker_cdc_task_failed stream={}", spec.streamId(), failure);
            }
            synchronized (this) {
                runner = null;
                if (cancelled.get()) {
                    terminalReport = cancelledReport();
                } else {
                    stagedFragments = fragments;
                    terminalReport = report;
                }
            }
        }

        private byte[] failure(TaskState state, ErrorClass errorClass, String code, String detail) {
            String message = detail == null || detail.isBlank() ? code : detail;
            if (message.length() > MAX_ERROR_MESSAGE_LENGTH) {
                message = message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
            }
            return WorkerTerminalTaskReports.workerExecutedFailure(request, state,
                    ExecutionError.newBuilder()
                            .setErrorClass(errorClass)
                            .setCode(code)
                            .setMessage(message)
                            .setFailedComponent("worker-cdc")
                            .build());
        }
    }

}
