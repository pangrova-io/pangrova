package org.pangrova.worker.copy;

import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import java.io.IOException;
import java.time.Clock;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Bounded, attempt-scoped staging before a COPY write claim is issued. */
public final class CopyStageLeases implements AutoCloseable {
    private static final int MAX_ATTEMPTS = 50_000;
    private static final int MAX_ACTIVE_STAGES = 8;
    private static final long EXPIRED_RETAIN_MILLIS = 10 * 60 * 1000L;

    @FunctionalInterface
    public interface Stager {
        CopySourceReadHttp.StagedSource stage(String nonce, CopySourceReadHttp.Scope scope)
                throws IOException;
    }

    @FunctionalInterface
    public interface ProofValidator {
        void verify(String nonce, CopySourceReadHttp.Scope scope,
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
    private boolean resourceCloseStarted;

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

    /** Returns the one durable proof for an exact canonical attempt and private nonce. */
    public CopySourceReadHttp.StagedSource stage(SubmitTaskRequest request) throws IOException {
        if (closed) throw new IllegalStateException("COPY staging is closed");
        if (request == null || request.copySourceRead() == null) {
            throw new IllegalArgumentException("COPY staging requires a bound private task ticket");
        }
        long now = clock.millis();
        long deadline = request.taskRequest().getDeadlineEpochMs();
        if (deadline <= now) throw new IllegalArgumentException("COPY staging deadline expired");
        var ticket = request.copySourceRead();
        AttemptKey key = new AttemptKey(ticket.scope().tenantId(),
                ticket.scope().executionId(), ticket.scope().stagingAttemptId());
        Lease candidate = new Lease(request);
        Lease existing;
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("COPY staging is closed");
            // The Coordinator's non-deletable, unique durable admission row is the
            // authority after local tombstones expire. Its old ticket has already
            // passed its deadline; a different ticket for this attempt cannot exist.
            if (leases.size() >= maxAttempts) {
                leases.entrySet().removeIf(entry -> entry.getValue().proof.isDone()
                        && entry.getValue().deadline <= now - EXPIRED_RETAIN_MILLIS);
            }
            existing = leases.get(key);
            if (existing == null) {
                if (leases.size() >= maxAttempts) {
                    throw new IllegalStateException("COPY staging lease capacity exceeded");
                }
                if (activeStages >= MAX_ACTIVE_STAGES) {
                    throw new IllegalStateException("COPY concurrent staging capacity exceeded");
                }
                leases.put(key, candidate);
                activeStages++;
            }
        }
        Lease lease = existing == null ? candidate : existing;
        if (!lease.matches(request)) {
            throw new IllegalArgumentException("COPY staging attempt identity conflict");
        }
        if (existing != null) {
            CopySourceReadHttp.StagedSource proof = await(lease.proof);
            validator.verify(ticket.nonce(), ticket.scope(), proof);
            return proof;
        }
        try {
            CopySourceReadHttp.StagedSource proof = Objects.requireNonNull(
                    stager.stage(ticket.nonce(), ticket.scope()), "COPY staged proof");
            candidate.proof.complete(proof);
            return proof;
        } catch (IOException | RuntimeException | Error failure) {
            candidate.proof.completeExceptionally(failure);
            synchronized (lifecycle) {
                leases.remove(key, candidate);
            }
            throw failure;
        } finally {
            synchronized (lifecycle) {
                activeStages--;
                lifecycle.notifyAll();
            }
        }
    }

    private static CopySourceReadHttp.StagedSource await(
            CompletableFuture<CopySourceReadHttp.StagedSource> proof) throws IOException {
        try {
            return proof.join();
        } catch (CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException("COPY staging failed", cause);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    @Override public void close() throws Exception {
        boolean interrupted = false;
        synchronized (lifecycle) {
            closed = true;
            while (activeStages > 0) {
                try {
                    lifecycle.wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (resourceCloseStarted) {
                if (interrupted) Thread.currentThread().interrupt();
                return;
            }
            resourceCloseStarted = true;
            leases.clear();
        }
        try {
            if (resource != null) resource.close();
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private static final class Lease {
        private final byte[] canonicalSha256;
        private final byte[] nonceSha256;
        private final CopySourceReadHttp.Scope scope;
        private final long deadline;
        private final CompletableFuture<CopySourceReadHttp.StagedSource> proof =
                new CompletableFuture<>();

        private Lease(SubmitTaskRequest request) {
            this.canonicalSha256 = sha256(request.canonicalBytes());
            this.nonceSha256 = sha256(request.copySourceRead().nonce()
                    .getBytes(StandardCharsets.UTF_8));
            this.scope = request.copySourceRead().scope();
            this.deadline = request.taskRequest().getDeadlineEpochMs();
        }

        private boolean matches(SubmitTaskRequest request) {
            return Arrays.equals(canonicalSha256, sha256(request.canonicalBytes()))
                    && MessageDigest.isEqual(nonceSha256,
                            sha256(request.copySourceRead().nonce().getBytes(StandardCharsets.UTF_8)))
                    && scope.equals(request.copySourceRead().scope());
        }
    }

    private record AttemptKey(String tenantId, String executionId, String attemptId) {}
}
