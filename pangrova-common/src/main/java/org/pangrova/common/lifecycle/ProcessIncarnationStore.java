package org.pangrova.common.lifecycle;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Durable Process execution incarnations, protected by a process-exclusive local directory lock.
 * Every incarnation is forced before it can admit work. Acquiring the same lock proves every
 * recorded predecessor has relinquished all execution ownership, either through a joined shutdown
 * or JVM termination. The directory must remain on one local filesystem and must never be copied,
 * replaced or shared with another independently running Process.
 */
@ThreadSafe
public final class ProcessIncarnationStore {
    private static final int MAGIC = 0x50474931;
    private static final int UUID_BYTES = 16;
    private static final int HASH_BYTES = 32;
    private static final int HEADER_BYTES = Integer.BYTES + UUID_BYTES + HASH_BYTES;
    private static final int RECORD_BYTES = UUID_BYTES + HASH_BYTES;
    private static final String LEDGER = "incarnations.bin";
    private static final Set<Path> OWNED_DIRECTORIES = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Identity identity;
    private final Set<String> retired;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final Set<String> authorities = new HashSet<>();
    private boolean authorityAdmissionSealed;
    private volatile boolean closed;

    private ProcessIncarnationStore(Path directory, FileChannel channel, FileLock lock,
                                            Identity identity, Set<String> retired) {
        this.directory = directory;
        this.lockChannel = channel;
        this.lock = lock;
        this.identity = identity;
        this.retired = Set.copyOf(retired);
    }

    /** Both components are opaque UUIDs; a Process ID or network address is not execution identity. */
    public record Identity(String ownershipDomainId, String incarnationId) {}

    /** Acquires exclusive ownership and durably publishes a fresh execution incarnation. */
    public static ProcessIncarnationStore open(Path directory) {
        FileChannel channel = null;
        FileLock lock = null;
        Path root = null;
        boolean claimed = false;
        try {
            Files.createDirectories(directory);
            root = directory.toRealPath();
            if (!OWNED_DIRECTORIES.add(root)) {
                throw new IllegalStateException("Process admission directory is already owned");
            }
            claimed = true;
            channel = FileChannel.open(root.resolve("ownership.lock"), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            lock = channel.tryLock();
            if (lock == null) throw new IllegalStateException("Process admission directory is already owned");
            syncDirectory(root);
            return initialize(root, channel, lock);
        } catch (IOException | RuntimeException failure) {
            closeFailedStart(lock, channel, failure);
            if (claimed) OWNED_DIRECTORIES.remove(root);
            if (failure instanceof IOException io) {
                throw new UncheckedIOException("Cannot open Process admission state", io);
            }
            throw (RuntimeException) failure;
        }
    }

    private static ProcessIncarnationStore initialize(Path root, FileChannel channel,
                                                              FileLock lock) throws IOException {
        Path ledger = root.resolve(LEDGER);
        if (!Files.exists(ledger, LinkOption.NOFOLLOW_LINKS)) createLedger(root, ledger);
        try (FileChannel data = FileChannel.open(ledger, StandardOpenOption.READ,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            readFully(data, header);
            byte[] encoded = header.array();
            byte[] previous = DigestUtils.sha256().digest(java.util.Arrays.copyOf(encoded, Integer.BYTES + UUID_BYTES));
            header.flip();
            if (header.getInt() != MAGIC) throw new IOException("Unknown Process incarnation ledger format");
            UUID domain = readUuid(header);
            byte[] stored = new byte[HASH_BYTES];
            header.get(stored);
            if (!MessageDigest.isEqual(previous, stored)) {
                throw new IOException("Corrupt Process incarnation ledger header");
            }
            Set<String> retired = new HashSet<>();
            while (data.size() - data.position() >= RECORD_BYTES) {
                ByteBuffer record = ByteBuffer.allocate(RECORD_BYTES);
                readFully(data, record);
                record.flip();
                UUID incarnation = readUuid(record);
                record.get(stored);
                previous = recordHash(previous, incarnation);
                if (!MessageDigest.isEqual(previous, stored) || !retired.add(incarnation.toString())) {
                    throw new IOException("Corrupt Process incarnation ledger record");
                }
            }
            // A torn final startup record was never durably published to an admission caller.
            // Complete records with a bad checksum are corruption, never a recoverable tail.
            if (data.position() != data.size()) data.truncate(data.position());
            UUID incarnation = UUID.randomUUID();
            ByteBuffer record = ByteBuffer.allocate(RECORD_BYTES);
            putUuid(record, incarnation);
            record.put(recordHash(previous, incarnation)).flip();
            writeFully(data, record);
            data.force(true);
            return new ProcessIncarnationStore(root, channel, lock,
                    new Identity(domain.toString(), incarnation.toString()), retired);
        }
    }

    private static void createLedger(Path root, Path ledger) throws IOException {
        Path pending = root.resolve("incarnations.pending");
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        header.putInt(MAGIC);
        putUuid(header, UUID.randomUUID());
        byte[] identity = java.util.Arrays.copyOf(header.array(), Integer.BYTES + UUID_BYTES);
        header.put(DigestUtils.sha256().digest(identity)).flip();
        try (FileChannel data = FileChannel.open(pending, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            writeFully(data, header);
            data.force(true);
        }
        Files.move(pending, ledger, StandardCopyOption.ATOMIC_MOVE);
        syncDirectory(root);
    }

    public Identity identity() {
        requireOwned();
        return identity;
    }

    /** Prevents rebuilding an in-memory admission authority under an already used incarnation. */
    public void claimAuthority(String authority) {
        synchronized (authorities) {
            requireOwned();
            if (authorityAdmissionSealed) {
                throw new IllegalStateException("Execution authority admission is sealed for shutdown");
            }
            if (authority == null || authority.isBlank() || !authorities.add(authority)) {
                throw new IllegalStateException("Execution authority is already claimed or invalid: " + authority);
            }
        }
    }

    /** Unknown domains/incarnations never establish physical exit. */
    public boolean isRetired(Identity target) {
        requireOwned();
        return identity.ownershipDomainId().equals(target.ownershipDomainId())
                && retired.contains(target.incarnationId());
    }

    /** Returns the immutable history from the currently owned local directory. */
    public Set<String> retiredIncarnations() {
        requireOwned();
        return retired;
    }

    /**
     * Runs the real shutdown join before releasing the exclusive lock. A failed join retains
     * ownership and can be retried. This is intentionally not AutoCloseable: generic resource
     * cleanup must not release physical-exit authority before all execution owners have joined.
     */
    public void closeAfterDrain(Runnable drain) {
        synchronized (authorities) {
            if (closed) return;
            authorityAdmissionSealed = true;
            if (!closing.compareAndSet(false, true)) {
                throw new IllegalStateException("Process incarnation shutdown is already running");
            }
        }
        try {
            drain.run();
            lock.release();
            lockChannel.close();
            closed = true;
            OWNED_DIRECTORIES.remove(directory);
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot release Process admission ownership", failure);
        } finally {
            closing.set(false);
        }
    }

    private void requireOwned() {
        if (closed || !lock.isValid()) throw new IllegalStateException("Process admission incarnation is closed");
    }

    private static byte[] recordHash(byte[] previous, UUID incarnation) {
        ByteBuffer value = ByteBuffer.allocate(HASH_BYTES + UUID_BYTES).put(previous);
        putUuid(value, incarnation);
        return DigestUtils.sha256().digest(value.array());
    }

    private static UUID readUuid(ByteBuffer value) {
        return new UUID(value.getLong(), value.getLong());
    }

    private static void putUuid(ByteBuffer value, UUID uuid) {
        value.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) throw new IOException("Truncated Process incarnation ledger header");
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void closeFailedStart(FileLock lock, FileChannel channel, Throwable failure) {
        try {
            if (lock != null) lock.release();
            if (channel != null) channel.close();
        } catch (IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
