package org.pangrova.shuffle.local.spool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import org.pangrova.shuffle.api.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;
import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolWriter;

/**
 * Exclusive local storage owner with bounded batch framing and exact attempt reads.
 * Synchronous storage I/O is serialized by the owner; asynchronous Provider sessions are separate.
 * This primitive does not supply exchange publication, network serving or restart recovery guarantees.
 */
public final class LocalFsSpoolStore implements SpoolStore {
    static final int DEFAULT_MAX_OPEN_FILES = 1024;
    final LocalSpoolPaths paths;
    private final int maxOpenFiles;
    private int reservedFiles;
    private final FileChannel ownerChannel;
    private final FileLock ownerLock;
    final Object monitor = new Object();
    private final IdentityHashMap<AutoCloseable, Reservation> resources = new IdentityHashMap<>();
    private final Set<Path> writers = new HashSet<>();
    private boolean closing;
    private boolean closed;

    /** Creates the local directory and holds its exclusive ownership lock until this store closes. */
    public LocalFsSpoolStore(Path baseDir) {
        this(baseDir, DEFAULT_MAX_OPEN_FILES);
    }

    /** Bounds open data files across all readers and writers; the root ownership lock is additional. */
    public LocalFsSpoolStore(Path baseDir, int maxOpenFiles) {
        if (baseDir == null) throw new IllegalArgumentException("baseDir must not be null");
        if (maxOpenFiles <= 0) throw new IllegalArgumentException("maxOpenFiles must be positive");
        this.maxOpenFiles = maxOpenFiles;
        FileChannel channel = null;
        try {
            paths = new LocalSpoolPaths(baseDir);
            channel = FileChannel.open(paths.lockFile(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            FileLock lock = channel.tryLock();
            if (lock == null) throw new IllegalStateException("Local spool directory already has an owner");
            paths.requireRoot();
            ownerChannel = channel;
            ownerLock = lock;
        } catch (IOException | RuntimeException failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof OverlappingFileLockException) {
                throw new IllegalStateException("Local spool directory already has an owner", failure);
            }
            if (failure instanceof IOException io) throw new UncheckedIOException("Cannot own spool directory", io);
            throw (RuntimeException) failure;
        }
    }

    @Override
    public SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
        synchronized (monitor) {
            requireOpen();
            requireFileBudget(1);
            try {
                var segment = paths.segment(queryId, stageId, partitionId, mapId, attemptId, true);
                Path path = segment.file();
                if (writers.contains(path) || Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalStateException("Spool attempt is already open or complete: " + path.getFileName());
                }
                var writer = new LocalFsSpoolWriter(this, segment);
                writers.add(path);
                register(writer, queryId, 1);
                try {
                    writer.initialize();
                    return writer;
                } catch (IOException | RuntimeException failure) {
                    try {
                        writer.close();
                    } catch (RuntimeException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                    throw failure;
                }
            } catch (IOException failure) {
                throw new UncheckedIOException("Cannot open spool writer", failure);
            }
        }
    }

    @Override
    public SpoolReader read(SpoolReadSelection selection) {
        java.util.Objects.requireNonNull(selection, "selection");
        synchronized (monitor) {
            requireOpen();
            LocalSpoolPaths.validateQueryId(selection.queryId());
            requireFileBudget(selection.producerAttempts().size());
            var reader = new LocalFsSpoolReader(this);
            register(reader, selection.queryId(), selection.producerAttempts().size());
            try {
                paths.requireRoot();
                for (var selected : selection.producerAttempts().entrySet()) {
                    var segment = paths.segment(selection.queryId(), selection.stageId(), selection.partitionId(),
                            selected.getKey(), selected.getValue(), false);
                    paths.requireCurrent(segment);
                    if (!Files.isRegularFile(segment.file(), LinkOption.NOFOLLOW_LINKS)) {
                        throw new IllegalStateException("Selected spool attempt is absent or incomplete");
                    }
                    reader.pin(segment);
                }
                return reader;
            } catch (IOException | RuntimeException failure) {
                try {
                    reader.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                if (failure instanceof java.nio.file.NoSuchFileException) {
                    throw new IllegalStateException("Selected spool attempt is absent or incomplete", failure);
                }
                if (failure instanceof IOException io) throw new UncheckedIOException("Cannot pin spool selection", io);
                throw (RuntimeException) failure;
            }
        }
    }

    @Override
    public void delete(String queryId) {
        synchronized (monitor) {
            requireOpen();
            LocalSpoolPaths.validateQueryId(queryId);
            if (resources.values().stream().anyMatch(reservation -> reservation.queryId().equals(queryId))) {
                throw new IllegalStateException("Spool query still has active readers or writers");
            }
            Path directory;
            try {
                directory = paths.queryDirectory(queryId);
                if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) return;
                paths.requireParents(directory);
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot validate the owned spool directory", failure);
            }
            try {
                Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException failure) throws IOException {
                        if (failure != null) throw failure;
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException failure) {
                throw new UncheckedIOException("Cannot delete spool data for " + queryId, failure);
            }
        }
    }

    @Override
    public ShuffleDurability durability() { return ShuffleDurability.LOCAL_SPILL; }

    @Override
    public void close() {
        synchronized (monitor) {
            if (closed) return;
            closing = true;
            RuntimeException failure = null;
            for (var resource : new ArrayList<>(resources.keySet())) {
                try {
                    resource.close();
                } catch (Exception closeFailure) {
                    if (failure == null) {
                        failure = new IllegalStateException("Cannot close spool resources", closeFailure);
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure != null) throw failure;
            try {
                if (ownerLock.isValid()) ownerLock.release();
                ownerChannel.close();
                closed = true;
            } catch (IOException closeFailure) {
                throw new UncheckedIOException("Cannot release spool directory ownership", closeFailure);
            }
        }
    }

    void release(AutoCloseable resource, Path writerPath) {
        Reservation reservation = resources.remove(resource);
        if (reservation != null) reservedFiles -= reservation.files();
        if (writerPath != null) writers.remove(writerPath);
    }

    void requireOpen() {
        if (closing || closed) throw new IllegalStateException("Spool store is closed");
    }

    private void requireFileBudget(int requested) {
        if (requested > maxOpenFiles - reservedFiles) {
            throw new IllegalStateException("Local spool open-file budget exhausted");
        }
    }

    private void register(AutoCloseable resource, String query, int files) {
        resources.put(resource, new Reservation(query, files));
        reservedFiles += files;
    }

    private record Reservation(String queryId, int files) {}
}
