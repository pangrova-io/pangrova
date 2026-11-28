package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** Recoverable I/O failure limited to real split files in the fixture's committed source table. */
@ThreadSafe
final class ProcessFileFault implements AutoCloseable {
    private final List<Path> authorizedFiles;
    private final CopyOnWriteArrayList<Move> moved = new CopyOnWriteArrayList<>();
    private final AtomicBoolean operation = new AtomicBoolean();

    ProcessFileFault(List<Path> actualSourceFiles) {
        authorizedFiles = actualSourceFiles.stream().map(path -> path.toAbsolutePath().normalize()).toList();
    }

    void hide(SubmitTaskRequest issued) throws IOException {
        claim();
        try { hideOwned(issued); }
        finally { operation.set(false); }
    }

    private void hideOwned(SubmitTaskRequest issued) throws IOException {
        if (issued.taskRequest().getSplitsCount() == 0) throw new IllegalArgumentException("No actual source split");
        for (var split : issued.taskRequest().getSplitsList()) {
            String location = split.getOpaqueLocation();
            Path decoded = location.startsWith("file:") ? Path.of(URI.create(location)) : Path.of(location);
            Path source = decoded.toAbsolutePath().normalize();
            if (!authorizedFiles.contains(source)) throw new IllegalArgumentException("Split is outside the owned source files");
            Path retained = source.resolveSibling(source.getFileName() + ".held");
            if (moved.stream().anyMatch(move -> move.source().equals(source))) continue;
            Files.move(source, retained, StandardCopyOption.ATOMIC_MOVE);
            moved.add(new Move(source, retained));
        }
    }

    @Override public void close() throws IOException {
        claim();
        try { restoreOwned(); }
        finally { operation.set(false); }
    }

    private void restoreOwned() throws IOException {
        IOException failure = null;
        for (Move move : moved) {
            try {
                Files.move(move.retained(), move.source(), StandardCopyOption.ATOMIC_MOVE);
                moved.remove(move);
            } catch (IOException restorationFailure) {
                if (failure == null) failure = restorationFailure;
                else failure.addSuppressed(restorationFailure);
            }
        }
        if (failure != null) throw failure;
    }

    private void claim() {
        if (!operation.compareAndSet(false, true)) throw new IllegalStateException("Actual source-file move is still in progress");
    }

    private record Move(Path source, Path retained) { }
}
