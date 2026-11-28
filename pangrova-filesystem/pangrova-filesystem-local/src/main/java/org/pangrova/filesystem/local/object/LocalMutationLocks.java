package org.pangrova.filesystem.local.object;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.TreeMap;

import org.pangrova.common.concurrency.ProcessResourceLocks;

/** Process admission precedes every lock-channel open; canonical OS locks coordinate independent JVMs. */
final class LocalMutationLocks implements AutoCloseable {
    static final String FILE_NAME = ".pangrova-mutation.lock";
    private static final FileChannel[] NO_CHANNELS = new FileChannel[0];
    private FileChannel[] channels = NO_CHANNELS;
    private List<Path> directories = List.of();
    private ProcessResourceLocks.Lease admission;

    static LocalMutationLocks acquire(Path... paths) throws IOException {
        var lease = new LocalMutationLocks();
        try {
            lease.lock(paths);
            return lease;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                lease.close();
            } catch (IOException | RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void lock(Path[] paths) throws IOException {
        var ordered = new TreeMap<String, Path>();
        for (Path path : paths) {
            Path parent = path.getParent();
            if (parent == null) throw new IOException("Mutation lock requires a parent directory");
            ordered.put(parent.toRealPath().resolve(path.getFileName()).toString(), path);
        }
        var targets = List.copyOf(ordered.values());
        channels = new FileChannel[targets.size()];
        directories = targets.stream().map(Path::getParent).toList();
        try {
            admission = ProcessResourceLocks.acquire(ordered.keySet());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            var interrupted = new InterruptedIOException("Interrupted waiting for local mutation admission");
            interrupted.initCause(failure);
            throw interrupted;
        }
        for (int index = 0; index < targets.size(); index++) {
            channels[index] = FileChannel.open(targets.get(index), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            // Only one channel for this inode can be open within the participating JVM cohort.
            // Its closure releases the OS lock before process admission can pass to the next owner.
            channels[index].lock();
        }
    }

    void forceDirectories() throws IOException {
        for (Path directory : directories) forceDirectory(directory);
    }

    static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        Throwable failure = null;
        try {
            for (int index = channels.length - 1; index >= 0; index--) {
                if (channels[index] == null) continue;
                try {
                    channels[index].close();
                } catch (IOException | RuntimeException | Error cleanup) {
                    if (failure == null) failure = cleanup;
                    else if (failure != cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
            }
        } finally {
            channels = NO_CHANNELS;
            directories = List.of();
            if (admission != null) {
                admission.close();
                admission = null;
            }
        }
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error fatal) throw fatal;
    }
}
