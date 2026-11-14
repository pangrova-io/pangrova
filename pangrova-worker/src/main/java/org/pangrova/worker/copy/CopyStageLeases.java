package org.pangrova.worker.copy;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.copy.PrepareCopySourceRequest;
import org.pangrova.protocol.v2.SourcePreparationResult;
import org.pangrova.protocol.v2.PreparedFileSource;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ErrorClass;

import java.io.IOException;
import java.time.Clock;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Bounded, attempt-scoped staging before a COPY write claim is issued. */
public final class CopyStageLeases implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CopyStageLeases.class);
    private static final int MAX_ATTEMPTS = 50_000;
    private static final int MAX_ACTIVE_STAGES = 8;
    private static final long EXPIRED_RETAIN_MILLIS = 10 * 60 * 1000L;

    @FunctionalInterface
    public interface Stager {
        CopySourceReadHttp.StagedSource stage(String nonce, CopySourceReadHttp.Scope scope, CopyPreparation budget)
                throws IOException;
    }

    @FunctionalInterface
    public interface ProofValidator {
        CopySourceReadHttp.StagedSource verify(String nonce, CopySourceReadHttp.Scope scope,
                CopySourceReadHttp.StagedSource proof);
    }

    @FunctionalInterface
    private interface Materializer {
        VerifiedLocalCopySource materialize(String nonce, CopySourceReadHttp.Scope scope,
                                            CopyMaterialization ownership) throws IOException;
    }

    private final Map<AttemptKey, Lease> leases = new HashMap<>();
    private final Stager stager;
    private final ProofValidator validator;
    private final Materializer materializer;
    private final Clock clock;
    private final AutoCloseable resource;
    private final int maxAttempts;
    private final Object lifecycle = new Object();
    private int activeStages;
    private volatile boolean closed;
    private boolean resourceClosed;
    private boolean closeInProgress;

    public CopyStageLeases(CopySourceStagingCommand command, AutoCloseable resource) {
        this(command::execute, command::verifyRecordedProof, command::executeForNative,
                Clock.systemUTC(), resource, MAX_ATTEMPTS);
    }

    public CopyStageLeases(Stager stager, ProofValidator validator,
                           Clock clock, AutoCloseable resource) {
        this(stager, validator, null, clock, resource, MAX_ATTEMPTS);
    }

    CopyStageLeases(Stager stager, ProofValidator validator, Clock clock,
                    AutoCloseable resource, int maxAttempts) {
        this(stager, validator, null, clock, resource, maxAttempts);
    }

    private CopyStageLeases(Stager stager, ProofValidator validator, Materializer materializer,
                            Clock clock, AutoCloseable resource, int maxAttempts) {
        this.stager = Objects.requireNonNull(stager, "stager");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.materializer = materializer;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.resource = resource;
        if (maxAttempts <= 0) throw new IllegalArgumentException("maxAttempts must be positive");
        this.maxAttempts = maxAttempts;
    }

    public boolean supportsNativeInputs() { return materializer != null; }

    /** Joins real materialization I/O with close before releasing the managed client. */
    public VerifiedLocalCopySource materialize(SubmitTaskRequest request, CopyMaterialization ownership)
            throws IOException {
        if (materializer == null) throw new IllegalStateException("COPY Native materializer is not configured");
        Objects.requireNonNull(request, "request");
        var ticket = Objects.requireNonNull(request.copySourceRead(), "private source ticket");
        if (request.taskRequest().getDeadlineEpochMs() <= clock.millis()) {
            throw new IllegalArgumentException("COPY materialization deadline expired");
        }
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("COPY staging is closed");
            if (activeStages >= MAX_ACTIVE_STAGES) {
                throw new IllegalStateException("COPY concurrent staging capacity exceeded");
            }
            activeStages++;
        }
        try {
            return Objects.requireNonNull(materializer.materialize(ticket.nonce(), ticket.scope(), ownership),
                    "COPY verified local input");
        } finally {
            synchronized (lifecycle) {
                activeStages--;
                lifecycle.notifyAll();
            }
        }
    }

    /** Pure identity publication; the TaskManager calls this inside exact admission publication. */
    public Admission admit(PrepareCopySourceRequest request) {
        var ticket = request.sourceRead();
        var key = new AttemptKey(ticket.scope().tenantId(), ticket.scope().executionId(),
                ticket.scope().stagingAttemptId());
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("COPY staging is closed");
            var existing = leases.get(key);
            if (existing != null) {
                if (!existing.matches(request)) {
                    throw new IllegalArgumentException("COPY staging attempt identity conflict");
                }
                return new Admission(existing);
            }
            long now = clock.millis();
            if (leases.size() >= maxAttempts) {
                leases.entrySet().removeIf(entry -> entry.getValue().result.isDone()
                        && entry.getValue().budget.resourcesReleased()
                        && entry.getValue().deadline <= now - EXPIRED_RETAIN_MILLIS);
            }
            if (leases.size() >= maxAttempts) throw new IllegalStateException("COPY staging lease capacity exceeded");
            long unfinished = leases.values().stream()
                    .filter(lease -> !lease.result.isDone() || !lease.budget.resourcesReleased()).count();
            if (unfinished >= MAX_ACTIVE_STAGES) {
                throw new IllegalStateException("COPY preparation owner capacity exceeded");
            }
            var lease = new Lease(request, clock);
            leases.put(key, lease);
            return new Admission(lease);
        }
    }

    /** Returns the one durable proof for an exact canonical attempt and private nonce. */
    SourcePreparationResult stage(PrepareCopySourceRequest request) throws IOException {
        return admit(request).execute(request);
    }

    private SourcePreparationResult execute(PrepareCopySourceRequest request, Lease candidate) throws IOException {
        if (request.preparation().getDeadlineEpochMs() <= clock.millis()) {
            throw new IllegalArgumentException("COPY staging deadline expired");
        }
        if (!candidate.matches(request)) throw new IllegalArgumentException("COPY staging attempt identity conflict");
        var ticket = request.sourceRead();
        boolean producer;
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("COPY staging is closed");
            if (activeStages >= MAX_ACTIVE_STAGES) {
                if (!candidate.started) {
                    candidate.started = true;
                    var refused = candidate.receipt().setError(ExecutionError.newBuilder()
                            .setErrorClass(ErrorClass.ERROR_CLASS_ADMISSION).setCode("COPY_SOURCE_CAPACITY")
                            .setMessage("Source preparation capacity is unavailable")
                            .setFailedComponent("worker-copy-source").setRetryable(false)).build();
                    candidate.result.complete(refused);
                    return refused;
                }
                return candidate.receipt().setIoJoined(false).setError(ExecutionError.newBuilder()
                        .setErrorClass(ErrorClass.ERROR_CLASS_ADMISSION).setCode("COPY_SOURCE_WAITING_CAPACITY")
                        .setMessage("Source preparation is reserved but has not entered I/O")
                        .setFailedComponent("worker-copy-source").setRetryable(true)).build();
            }
            producer = !candidate.started;
            candidate.started = true;
            activeStages++;
            candidate.activeCalls++;
        }
        if (!producer) {
            try {
                var awaited = await(candidate, request);
                var result = awaited.result();
                if (!awaited.producerJoined()) return result;
                if (!result.getIoJoined()) candidate.budget.cleanupResources();
                validator.verify(ticket.nonce(), ticket.scope(), result.hasSource() ? proof(result.getSource()) : null);
                return result.toBuilder().setIoJoined(
                        candidate.result.isDone() && candidate.budget.resourcesReleased()).build();
            } finally {
                activityExited(candidate);
            }
        }
        try {
            var source = Objects.requireNonNull(stager.stage(ticket.nonce(), ticket.scope(), candidate.budget),
                    "COPY staged proof");
            candidate.budget.checkpoint();
            if (!candidate.budget.cleanupResources()) throw new IOException("COPY preparation cleanup has not joined");
            var result = candidate.receipt().setSource(PreparedFileSource.newBuilder()
                    .setSourceVersionId(source.sourceVersionId()).setManagedObjectUri(source.managedObjectUri())
                    .setContentSha256(source.contentSha256()).setContentSize(source.contentSize())
                    .setTenantScope(source.tenantScope())).build();
            candidate.result.complete(result);
            return result;
        } catch (IOException | RuntimeException failure) {
            LOG.warn("event=copy_preparation_failed taskId={} queryId={}",
                    request.preparation().getAttempt().getTaskId(),
                    request.preparation().getAttempt().getQueryOrJobId(), failure);
            var result = candidate.receipt().setError(ExecutionError.newBuilder()
                    .setErrorClass(failureCategory(failure))
                    .setCode("COPY_SOURCE_PREPARATION_FAILED").setMessage("COPY source preparation failed")
                    .setFailedComponent("worker-copy-source").setRetryable(false)).build();
            candidate.result.complete(result);
            // The issued allowance and terminal failure stay immutable; a repeat never restarts I/O.
            return result;
        } catch (Error failure) {
            candidate.result.complete(candidate.receipt().setError(ExecutionError.newBuilder()
                    .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL).setCode("COPY_PREPARATION_FATAL")
                    .setMessage("Source preparation terminated abnormally")
                    .setFailedComponent("worker-copy-source")).build());
            throw failure;
        } finally {
            activityExited(candidate);
        }
    }

    private static ErrorClass failureCategory(Exception failure) {
        if (failure instanceof CopyPreparation.Halted halted) return halted.category();
        if (failure instanceof org.pangrova.security.api.exception.AccessDeniedException
                || failure instanceof CopySourceReadClient.Rejected rejected && rejected.isPolicyDenial()) {
            return ErrorClass.ERROR_CLASS_POLICY_DENIED;
        }
        return ErrorClass.ERROR_CLASS_TRANSPORT;
    }

    private void activityExited(Lease lease) {
        synchronized (lifecycle) {
            activeStages--;
            lease.activeCalls--;
            lifecycle.notifyAll();
        }
    }

    private static CopySourceReadHttp.StagedSource proof(PreparedFileSource source) {
        return new CopySourceReadHttp.StagedSource(source.getSourceVersionId(), source.getManagedObjectUri(),
                source.getContentSha256(), source.getContentSize(), source.getTenantScope());
    }

    private record Awaited(SourcePreparationResult result, boolean producerJoined) {}

    private Awaited await(Lease lease, PrepareCopySourceRequest request) throws IOException {
        try {
            return new Awaited(lease.result.get(Math.max(1, lease.deadline - clock.millis()),
                    java.util.concurrent.TimeUnit.MILLISECONDS), true);
        } catch (java.util.concurrent.TimeoutException expired) {
            return new Awaited(lease.receipt().setIoJoined(false).setError(ExecutionError.newBuilder()
                    .setErrorClass(ErrorClass.ERROR_CLASS_DEADLINE).setCode("COPY_SOURCE_STILL_ACTIVE")
                    .setMessage("Source preparation has not physically joined")
                    .setFailedComponent("worker-copy-source")).build(), false);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for source preparation", interrupted);
        } catch (java.util.concurrent.ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException("COPY staging failed", cause);
        }
    }

    @Override public void close() throws Exception {
        boolean interrupted = false;
        java.util.List<Lease> owned;
        synchronized (lifecycle) {
            closed = true;
            for (var lease : leases.values()) new Admission(lease).cancel();
            while (activeStages > 0 || closeInProgress) {
                try {
                    lifecycle.wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (resourceClosed) {
                if (interrupted) Thread.currentThread().interrupt();
                return;
            }
            closeInProgress = true;
            owned = java.util.List.copyOf(leases.values());
        }
        try {
            for (var lease : owned) {
                if (!lease.budget.cleanupResources()) throw new IOException("COPY draft cleanup is still active");
            }
            if (resource != null) resource.close();
            synchronized (lifecycle) {
                resourceClosed = true;
            }
        } finally {
            synchronized (lifecycle) {
                closeInProgress = false;
                lifecycle.notifyAll();
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /** Replays an already sealed physical outcome; only the Coordinator authority is contacted. */
    public SourcePreparationResult replay(PrepareCopySourceRequest request,
                                          org.pangrova.protocol.v2.ResourceUsage usage,
                                          ExecutionError error, boolean succeeded) {
        var ticket = request.sourceRead();
        var current = validator.verify(ticket.nonce(), ticket.scope(), null);
        var result = SourcePreparationResult.newBuilder().setAttempt(request.preparation().getAttempt())
                .setRequestSha256(request.admissionSeal().getCanonicalRequestChecksum())
                .setUsage(usage).setIoJoined(true);
        if (succeeded) {
            if (current == null) throw new IllegalStateException("Completed source preparation has no durable proof");
            result.setSource(PreparedFileSource.newBuilder().setSourceVersionId(current.sourceVersionId())
                    .setManagedObjectUri(current.managedObjectUri()).setContentSha256(current.contentSha256())
                    .setContentSize(current.contentSize()).setTenantScope(current.tenantScope()));
        } else {
            result.setError(error);
        }
        return result.build();
    }

    /** Exact in-process preparation owner, including cancelled-before-entry and cleanup retries. */
    public final class Admission {
        private final Lease lease;

        private Admission(Lease lease) { this.lease = lease; }

        public SourcePreparationResult execute(PrepareCopySourceRequest request) throws IOException {
            return CopyStageLeases.this.execute(request, lease);
        }

        /** Seals this owner without claiming that an active producer has exited. */
        public void cancel() {
            synchronized (lifecycle) {
                lease.budget.cancel();
                if (!lease.started) {
                    lease.started = true;
                    lease.result.complete(lease.receipt().setError(ExecutionError.newBuilder()
                            .setErrorClass(ErrorClass.ERROR_CLASS_CANCELLED).setCode("COPY_PREPARATION_SEALED")
                            .setMessage("Source preparation admission is closed")
                            .setFailedComponent("worker-copy-source")).build());
                }
            }
        }

        /** A missing value is not drain evidence. No new source read is performed here. */
        public java.util.Optional<SourcePreparationResult> drain() throws IOException {
            synchronized (lifecycle) {
                if (lease.activeCalls > 0 || !lease.result.isDone()) return java.util.Optional.empty();
            }
            var result = lease.result.getNow(SourcePreparationResult.getDefaultInstance());
            if (!lease.budget.cleanupResources()) return java.util.Optional.empty();
            return java.util.Optional.of(result.toBuilder().setIoJoined(true).build());
        }

        /** Removes a fully settled local owner after its shared terminal record has been retained. */
        public void retire() {
            synchronized (lifecycle) {
                if (!lease.result.isDone() || !lease.budget.resourcesReleased()) {
                    throw new IllegalStateException("Active COPY preparation owner cannot be retired");
                }
                leases.remove(new AttemptKey(lease.scope.tenantId(), lease.scope.executionId(),
                        lease.scope.stagingAttemptId()), lease);
            }
        }
    }

    private static final class Lease {
        private final byte[] canonicalSha256;
        private final byte[] nonceSha256;
        private final CopySourceReadHttp.Scope scope;
        private final long deadline;
        private final CopyPreparation budget;
        private final org.pangrova.protocol.v2.TaskAdmissionSealRequest authority;
        private boolean started;
        private int activeCalls;
        private final CompletableFuture<SourcePreparationResult> result = new CompletableFuture<>();

        private Lease(PrepareCopySourceRequest request, Clock clock) {
            this.canonicalSha256 = DigestUtils.sha256().digest(request.canonicalBytes());
            this.nonceSha256 = DigestUtils.sha256().digest(request.sourceRead().nonce()
                    .getBytes(StandardCharsets.UTF_8));
            this.scope = request.sourceRead().scope();
            this.deadline = request.preparation().getDeadlineEpochMs();
            this.budget = new CopyPreparation(request.preparation().getResourceGrant(), deadline, clock);
            this.authority = request.admissionSeal();
        }

        private SourcePreparationResult.Builder receipt() {
            return SourcePreparationResult.newBuilder().setAttempt(authority.getAttempt())
                    .setRequestSha256(authority.getCanonicalRequestChecksum())
                    .setUsage(budget.usage()).setIoJoined(budget.resourcesReleased());
        }

        private boolean matches(PrepareCopySourceRequest request) {
            return Arrays.equals(canonicalSha256, DigestUtils.sha256().digest(request.canonicalBytes()))
                    && MessageDigest.isEqual(nonceSha256,
                            DigestUtils.sha256().digest(request.sourceRead().nonce().getBytes(StandardCharsets.UTF_8)))
                    && scope.equals(request.sourceRead().scope());
        }
    }

    private record AttemptKey(String tenantId, String executionId, String attemptId) {}
}
