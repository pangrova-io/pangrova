package org.pangrova.worker.copy;

import org.pangrova.worker.exception.CopyPreparationHaltedException;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;

import java.time.Clock;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** One preparation's cumulative requested payloads and private-draft reservation. */
@org.pangrova.common.concurrent.ThreadSafe
public final class CopyPreparation {
    private final StageResourceGrant grant;
    private final long deadline;
    private final Clock clock;
    private long readBytes;
    private long writeBytes;
    private long spillBytes;
    private boolean cancelled;
    private Path draft;
    private boolean cleaningDraft;
    private AutoCloseable sourceClient;
    private boolean closingClient;

    public CopyPreparation(StageResourceGrant grant, long deadline, Clock clock) {
        this.grant = Objects.requireNonNull(grant, "grant");
        this.deadline = deadline;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Refuses the known source+readback+publication pipeline before any payload request starts. */
    public synchronized void requireStageCapacity(long size) {
        if (grant.getJvmHeapBytes() < 64 * 1024) {
            throw new CopyPreparationHaltedException(ErrorClass.ERROR_CLASS_BUDGET_EXHAUSTED, "COPY preparation stream buffer is not admitted");
        }
        if (size < 0 || size > Long.MAX_VALUE / 2) {
            throw new CopyPreparationHaltedException(ErrorClass.ERROR_CLASS_BUDGET_EXHAUSTED, "COPY source size exceeds accounting domain");
        }
        consume(readBytes, grant.getObjectStoreReadBytes(), 2 * size);
        consume(writeBytes, grant.getObjectStoreWriteBytes(), size);
        consume(spillBytes, grant.getSpillBytes(), size);
    }

    public synchronized void reserveRead(long bytes) {
        readBytes = consume(readBytes, grant.getObjectStoreReadBytes(), bytes);
    }
    public synchronized void reserveWrite(long bytes) {
        writeBytes = consume(writeBytes, grant.getObjectStoreWriteBytes(), bytes);
    }
    public synchronized void reserveDraft(long bytes) {
        spillBytes = consume(spillBytes, grant.getSpillBytes(), bytes);
    }

    private long consume(long consumed, long limit, long bytes) {
        checkpoint();
        if (bytes < 0 || limit < 0 || bytes > limit - consumed) {
            throw new CopyPreparationHaltedException(ErrorClass.ERROR_CLASS_BUDGET_EXHAUSTED, "COPY preparation payload grant exhausted");
        }
        return consumed + bytes;
    }

    /** Checks cancellation and the issued deadline before entering another I/O operation. */
    public synchronized void checkpoint() {
        if (cancelled || Thread.currentThread().isInterrupted()) {
            throw new CopyPreparationHaltedException(ErrorClass.ERROR_CLASS_CANCELLED, "COPY source preparation cancelled");
        }
        if (clock.millis() >= deadline) {
            throw new CopyPreparationHaltedException(ErrorClass.ERROR_CLASS_DEADLINE, "COPY source preparation deadline expired");
        }
    }

    /** Publishes the empty draft's owner before any body bytes enter it. */
    public synchronized void ownDraft(Path path) {
        if (draft != null) throw new IllegalStateException("COPY preparation already owns a draft");
        draft = Objects.requireNonNull(path, "draft");
    }

    /** A failed unlink retains its exact owner; concurrent cleanup cannot claim another thread's exit. */
    public boolean cleanupDraft() throws IOException {
        Path owned;
        synchronized (this) {
            if (cleaningDraft) return false;
            owned = draft;
            if (owned == null) return true;
            cleaningDraft = true;
        }
        boolean deleted = false;
        try {
            Files.deleteIfExists(owned);
            deleted = true;
            return true;
        } finally {
            synchronized (this) {
                if (deleted) draft = null;
                cleaningDraft = false;
            }
        }
    }

    public synchronized void ownSourceClient(AutoCloseable client) {
        if (sourceClient != null) throw new IllegalStateException("COPY preparation already owns a source client");
        sourceClient = Objects.requireNonNull(client, "sourceClient");
    }

    /** Retries the exact client close, retaining ownership if it fails. */
    public boolean cleanupSourceClient() throws IOException {
        AutoCloseable owned;
        synchronized (this) {
            if (closingClient) return false;
            owned = sourceClient;
            if (owned == null) return true;
            closingClient = true;
        }
        boolean released = false;
        try {
            owned.close();
            released = true;
            return true;
        } catch (Exception failure) {
            throw new IOException("COPY source client cleanup failed", failure);
        } finally {
            synchronized (this) {
                if (released) sourceClient = null;
                closingClient = false;
            }
        }
    }

    /** Attempts independent client and draft cleanup and preserves every failure. */
    public boolean cleanupResources() throws IOException {
        IOException failure = null;
        boolean client = false;
        try {
            client = cleanupSourceClient();
        } catch (IOException pending) {
            failure = pending;
        }
        boolean file = false;
        try {
            file = cleanupDraft();
        } catch (IOException pending) {
            if (failure == null) failure = pending;
            else failure.addSuppressed(pending);
        }
        if (failure != null) throw failure;
        return client && file;
    }

    public synchronized boolean resourcesReleased() {
        return draft == null && !cleaningDraft && sourceClient == null && !closingClient;
    }

    public synchronized void cancel() { cancelled = true; }

    public synchronized ResourceUsage usage() {
        return ResourceUsage.newBuilder().setObjectStoreReadBytes(readBytes)
                .setObjectStoreWriteBytes(writeBytes).setSpillBytes(spillBytes).build();
    }

}
